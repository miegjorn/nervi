/**
 * Nèrvi SRE watcher (N-5) — Kubernetes adapter.
 *
 * The only module that imports `@kubernetes/client-node`. It implements the
 * PodSource seam over the cluster API: listing the pod/container targets that
 * match the watch config, and following a pod's logs as an async iterable of
 * parsed lines. All resume / reconnect / rotation policy lives in watcher.ts —
 * this layer is a thin, mechanical bridge to the API server.
 */
import * as readline from 'node:readline';
import { PassThrough } from 'node:stream';
import { CoreV1Api, KubeConfig, Log } from '@kubernetes/client-node';
import {
  parseLogLine,
  type PodRef,
  type PodSource,
  type RawLogLine,
  type StreamLogsOptions,
  type WatchConfig,
} from './core.js';

/** Build a KubeConfig, preferring the in-cluster ServiceAccount, then kubeconfig. */
export function loadKubeConfig(): KubeConfig {
  const kc = new KubeConfig();
  try {
    kc.loadFromCluster();
  } catch {
    kc.loadFromDefault();
  }
  return kc;
}

export class K8sPodSource implements PodSource {
  private readonly core: CoreV1Api;
  private readonly logApi: Log;

  constructor(
    kc: KubeConfig,
    private readonly config: WatchConfig,
  ) {
    this.core = kc.makeApiClient(CoreV1Api);
    this.logApi = new Log(kc);
  }

  /**
   * List running pods matching the watch config, expanded to one PodRef per
   * container (or only the configured container). Non-running pods are skipped
   * — their logs are picked up once they reach Running on a later reconcile.
   */
  async listTargets(): Promise<PodRef[]> {
    const labelSelector = this.config.labelSelector;
    const lists = this.config.namespaces.length
      ? await Promise.all(
          this.config.namespaces.map((namespace) =>
            this.core.listNamespacedPod({ namespace, labelSelector }),
          ),
        )
      : [await this.core.listPodForAllNamespaces({ labelSelector })];

    const refs: PodRef[] = [];
    for (const list of lists) {
      for (const pod of list.items) {
        if (pod.status?.phase !== 'Running') continue;
        const namespace = pod.metadata?.namespace;
        const name = pod.metadata?.name;
        if (!namespace || !name) continue;
        for (const c of pod.spec?.containers ?? []) {
          if (this.config.container && c.name !== this.config.container) continue;
          refs.push({ namespace, pod: name, container: c.name });
        }
      }
    }
    return refs;
  }

  /**
   * Follow a pod/container's logs, yielding one parsed line at a time. The
   * Kubernetes log API streams into a Writable and ends it on EOF; we read that
   * with `readline` (itself an async iterable) and parse off the RFC3339 prefix.
   * The generator completing signals the stream ended — the watcher reconnects.
   */
  async *streamLogs(ref: PodRef, opts: StreamLogsOptions): AsyncIterable<RawLogLine> {
    const stream = new PassThrough();
    // sinceTime and sinceSeconds are mutually exclusive in the API; the resume
    // cursor (sinceTime) wins once we have one.
    const logOptions = {
      follow: true,
      timestamps: true,
      ...(opts.sinceTime
        ? { sinceTime: opts.sinceTime }
        : opts.sinceSeconds
          ? { sinceSeconds: opts.sinceSeconds }
          : {}),
    };

    const controller = await this.logApi.log(
      ref.namespace,
      ref.pod,
      ref.container,
      stream,
      logOptions,
    );

    const onAbort = () => {
      controller.abort();
      stream.destroy();
    };
    if (opts.signal.aborted) onAbort();
    else opts.signal.addEventListener('abort', onAbort, { once: true });

    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    try {
      for await (const line of rl) {
        yield parseLogLine(line);
      }
    } finally {
      opts.signal.removeEventListener('abort', onAbort);
      rl.close();
      controller.abort();
      stream.destroy();
    }
  }
}
