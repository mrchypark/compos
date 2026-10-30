use std::{env, error::Error, fmt, sync::Arc, time::Duration};

use anyhow::{anyhow, bail};
use futures::StreamExt;
use k8s_openapi::api::{
    apps::v1::{Deployment, StatefulSet},
    core::v1::{Secret, Service},
};
use kube::{
    Api, Client, CustomResource, CustomResourceExt, Resource, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "compos.mrchypark.dev",
    version = "v1alpha1",
    kind = "ComposCluster",
    plural = "clusters",
    namespaced,
    status = "ComposClusterStatus",
    shortname = "cc"
)]
struct ComposClusterSpec {
    server: ServerSpec,
    #[serde(rename = "objectStore", default)]
    object_store: ObjectStoreSpec,
    #[serde(default = "default_replicas")]
    replicas: i32,
    #[serde(rename = "imagePullPolicy", default = "default_image_pull_policy")]
    image_pull_policy: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerSpec {
    #[serde(rename = "envSecret", default = "default_env_secret")]
    env_secret: String,
    image: String,
    #[serde(default = "default_server_port")]
    port: i32,
    #[serde(default)]
    rhiza: RhizaSpec,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct RhizaSpec {
    #[serde(rename = "clusterId", default = "default_cluster_id")]
    cluster_id: String,
    #[serde(rename = "peerPort", default = "default_peer_port")]
    peer_port: i32,
    #[serde(rename = "adminTokenSecretRef", default = "default_env_secret")]
    admin_token_secret_ref: String,
}

impl Default for RhizaSpec {
    fn default() -> Self {
        Self {
            cluster_id: default_cluster_id(),
            peer_port: default_peer_port(),
            admin_token_secret_ref: default_env_secret(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectStoreSpec {
    #[serde(default = "default_object_provider")]
    provider: String,
    #[serde(default = "default_object_endpoint")]
    endpoint: String,
    #[serde(default = "default_object_bucket")]
    bucket: String,
    #[serde(default = "default_object_prefix")]
    prefix: String,
    #[serde(default = "default_region")]
    region: String,
    #[serde(default = "default_true")]
    insecure: bool,
    #[serde(rename = "secretRef", default = "default_env_secret")]
    secret_ref: String,
}

impl Default for ObjectStoreSpec {
    fn default() -> Self {
        Self {
            provider: default_object_provider(),
            endpoint: default_object_endpoint(),
            bucket: default_object_bucket(),
            prefix: default_object_prefix(),
            region: default_region(),
            insecure: true,
            secret_ref: default_env_secret(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposClusterStatus {
    phase: Option<String>,
    ready_replicas: Option<i32>,
    observed_generation: Option<i64>,
    last_error: Option<String>,
}

#[derive(Clone)]
struct Context {
    client: Client,
}

#[derive(Debug)]
struct OperatorError(String);

impl fmt::Display for OperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for OperatorError {}

impl From<anyhow::Error> for OperatorError {
    fn from(value: anyhow::Error) -> Self {
        Self(format!("{value:#}"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if env::args().any(|arg| arg == "--print-cluster-crd") {
        println!("{}", serde_json::to_string_pretty(&ComposCluster::crd())?);
        return Ok(());
    }
    if env::args().any(|arg| arg == "--print-search-syncer-crd") {
        println!(
            "{}",
            serde_json::to_string_pretty(&ComposSearchSyncer::crd())?
        );
        return Ok(());
    }
    let client = Client::try_default().await?;
    let namespace = env::var("COMPOS_OPERATOR_NAMESPACE").unwrap_or_else(|_| "compos".to_string());
    let clusters: Api<ComposCluster> = Api::namespaced(client.clone(), &namespace);
    let search_syncers: Api<ComposSearchSyncer> = Api::namespaced(client.clone(), &namespace);
    let context = Arc::new(Context { client });

    log(&format!("started namespace={namespace}"));

    let cluster_controller = Controller::new(clusters, watcher::Config::default())
        .run(reconcile, error_policy, context.clone())
        .for_each(|result| async move {
            if let Err(err) = result {
                log(&format!("cluster reconcile failed: {err}"));
            }
        });
    let syncer_controller = Controller::new(search_syncers, watcher::Config::default())
        .run(reconcile_search_syncer, error_policy_search_syncer, context)
        .for_each(|result| async move {
            if let Err(err) = result {
                log(&format!("search syncer reconcile failed: {err}"));
            }
        });

    tokio::join!(cluster_controller, syncer_controller);
    Ok(())
}

async fn reconcile(
    cluster: Arc<ComposCluster>,
    ctx: Arc<Context>,
) -> Result<Action, OperatorError> {
    reconcile_inner(cluster, ctx).await.map_err(Into::into)
}

async fn reconcile_inner(cluster: Arc<ComposCluster>, ctx: Arc<Context>) -> anyhow::Result<Action> {
    let namespace = cluster
        .namespace()
        .ok_or_else(|| anyhow!("ComposCluster must be namespaced"))?;
    let name = cluster.name_any();
    let spec = &cluster.spec;

    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
    let clusters: Api<ComposCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    if let Err(err) = validate_spec(spec) {
        patch_status(&clusters, &name, &invalid_cluster_status(&cluster, &err)).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    apply_service(&services, &headless_service(&cluster, spec)).await?;
    apply_service(&services, &http_service(&cluster, spec)).await?;
    apply_statefulset(&statefulsets, &compos_statefulset(&cluster, spec)).await?;

    let ready = statefulset_ready_replicas(&statefulsets, &name).await?;
    let healthy = ready == spec.replicas;
    let status = ComposClusterStatus {
        phase: Some(if healthy { "Running" } else { "Deploying" }.to_string()),
        ready_replicas: Some(ready),
        observed_generation: cluster.metadata.generation,
        last_error: None,
    };
    patch_status(&clusters, &name, &status).await?;
    Ok(Action::requeue(Duration::from_secs(10)))
}

fn error_policy(_cluster: Arc<ComposCluster>, _err: &OperatorError, _ctx: Arc<Context>) -> Action {
    Action::requeue(Duration::from_secs(30))
}

fn error_policy_search_syncer(
    _syncer: Arc<ComposSearchSyncer>,
    _err: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    Action::requeue(Duration::from_secs(30))
}

fn validate_spec(spec: &ComposClusterSpec) -> anyhow::Result<()> {
    if spec.replicas < 3 || spec.replicas % 2 == 0 {
        bail!("spec.replicas must be an odd number of at least 3");
    }
    if spec.server.port < 1 || spec.server.rhiza.peer_port < 1 {
        bail!("server and Rhiza peer ports must be positive");
    }
    if spec.server.image.trim().is_empty() {
        bail!("spec.server.image is required");
    }
    if spec.server.rhiza.cluster_id.trim().is_empty() {
        bail!("spec.server.rhiza.clusterId is required");
    }
    if spec.object_store.endpoint.trim().is_empty() || spec.object_store.bucket.trim().is_empty() {
        bail!("spec.objectStore.endpoint and bucket are required");
    }
    Ok(())
}

fn invalid_cluster_status(cluster: &ComposCluster, err: &anyhow::Error) -> ComposClusterStatus {
    ComposClusterStatus {
        phase: Some("Invalid".to_string()),
        last_error: Some(err.to_string()),
        observed_generation: cluster.metadata.generation,
        ..cluster.status.clone().unwrap_or_default()
    }
}

fn labels(name: &str) -> Value {
    json!({"app.kubernetes.io/name": "compos", "app.kubernetes.io/instance": name})
}

fn rhiza_members(name: &str, spec: &ComposClusterSpec) -> String {
    let members = (0..spec.replicas)
        .map(|ordinal| {
            let host = format!("{name}-{ordinal}.{name}-headless");
            json!({
                "node_id": format!("{name}-{ordinal}"),
                "url": format!("http://{host}:{}", spec.server.port),
                "peer_url": format!("quic://{host}:{}", spec.server.rhiza.peer_port),
                "log_url": format!("http://{host}:{}", spec.server.port)
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&members).expect("Rhiza members serialize")
}

fn cluster_owner_references(cluster: &ComposCluster) -> Value {
    serde_json::to_value([cluster.owner_ref(&()).expect("CR owner reference")])
        .expect("owner reference serializes")
}

fn headless_service(cluster: &ComposCluster, spec: &ComposClusterSpec) -> Value {
    let name = cluster.name_any();
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": format!("{name}-headless"), "labels": labels(&name), "ownerReferences": cluster_owner_references(cluster)},
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": labels(&name),
            "ports": [
                {"name": "http", "port": spec.server.port, "targetPort": "http"},
                {"name": "rhiza-peer", "port": spec.server.rhiza.peer_port, "targetPort": "rhiza-peer", "protocol": "UDP"}
            ]
        }
    })
}

fn http_service(cluster: &ComposCluster, spec: &ComposClusterSpec) -> Value {
    let name = cluster.name_any();
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": labels(&name), "ownerReferences": cluster_owner_references(cluster)},
        "spec": {
            "selector": labels(&name),
            "ports": [{"name": "http", "port": spec.server.port, "targetPort": "http"}]
        }
    })
}

fn compos_statefulset(cluster: &ComposCluster, spec: &ComposClusterSpec) -> Value {
    let name = cluster.name_any();
    let rhiza = &spec.server.rhiza;
    let object = &spec.object_store;
    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "labels": labels(&name), "ownerReferences": cluster_owner_references(cluster)},
        "spec": {
            "serviceName": format!("{name}-headless"),
            "replicas": spec.replicas,
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": labels(&name)},
            "template": {
                "metadata": {"labels": labels(&name)},
                "spec": {
                    "securityContext": {"fsGroup": 10001, "fsGroupChangePolicy": "OnRootMismatch"},
                    "volumes": [{"name": "data", "emptyDir": {}}],
                    "containers": [{
                        "name": "compos-server",
                        "image": spec.server.image,
                        "imagePullPolicy": spec.image_pull_policy,
                        "envFrom": [{"secretRef": {"name": spec.server.env_secret}}],
                        "env": [
                            {"name": "COMPOS_RHIZA_DATA_DIR", "value": "/data/rhiza"},
                            {"name": "COMPOS_RHIZA_CLUSTER_ID", "value": rhiza.cluster_id},
                            {"name": "COMPOS_RHIZA_NODE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                            {"name": "COMPOS_RHIZA_PEER_ADDR", "value": format!("0.0.0.0:{}", rhiza.peer_port)},
                            {"name": "COMPOS_RHIZA_MEMBERS", "value": rhiza_members(&name, spec)},
                            {"name": "COMPOS_RHIZA_ADMIN_TOKEN", "valueFrom": {"secretKeyRef": {"name": rhiza.admin_token_secret_ref, "key": "COMPOS_RHIZA_ADMIN_TOKEN"}}},
                            {"name": "COMPOS_RHIZA_OBJECT_PROVIDER", "value": object.provider},
                            {"name": "COMPOS_RHIZA_OBJECT_ENDPOINT", "value": object.endpoint},
                            {"name": "COMPOS_RHIZA_OBJECT_BUCKET", "value": object.bucket},
                            {"name": "COMPOS_RHIZA_OBJECT_PREFIX", "value": object.prefix},
                            {"name": "COMPOS_RHIZA_OBJECT_REGION", "value": object.region},
                            {"name": "COMPOS_RHIZA_OBJECT_INSECURE", "value": object.insecure.to_string()},
                            {"name": "COMPOS_RHIZA_OBJECT_DURABILITY", "value": "before-ack"},
                            {"name": "COMPOS_RHIZA_OBJECT_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": object.secret_ref, "key": "COMPOS_RHIZA_OBJECT_ACCESS_KEY"}}},
                            {"name": "COMPOS_RHIZA_OBJECT_SECRET_KEY", "valueFrom": {"secretKeyRef": {"name": object.secret_ref, "key": "COMPOS_RHIZA_OBJECT_SECRET_KEY"}}}
                        ],
                        "ports": [
                            {"name": "http", "containerPort": spec.server.port, "protocol": "TCP"},
                            {"name": "rhiza-peer", "containerPort": rhiza.peer_port, "protocol": "UDP"}
                        ],
                        "readinessProbe": {"httpGet": {"path": "/api/v1/health", "port": "http"}, "initialDelaySeconds": 5, "periodSeconds": 5},
                        "volumeMounts": [{"name": "data", "mountPath": "/data"}]
                    }]
                }
            }
        }
    })
}

async fn apply_service(api: &Api<Service>, value: &Value) -> anyhow::Result<()> {
    let name = value
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap();
    api.patch(name, &apply_params(), &Patch::Apply(value))
        .await?;
    Ok(())
}

async fn apply_statefulset(api: &Api<StatefulSet>, value: &Value) -> anyhow::Result<()> {
    let name = value
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap();
    api.patch(name, &apply_params(), &Patch::Apply(value))
        .await?;
    Ok(())
}

async fn apply_deployment(api: &Api<Deployment>, value: &Value) -> anyhow::Result<()> {
    let name = value
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap();
    api.patch(name, &apply_params(), &Patch::Apply(value))
        .await?;
    Ok(())
}

async fn statefulset_ready_replicas(api: &Api<StatefulSet>, name: &str) -> anyhow::Result<i32> {
    Ok(api
        .get_opt(name)
        .await?
        .and_then(|set| set.status.and_then(|status| status.ready_replicas))
        .unwrap_or(0))
}

async fn patch_status(
    api: &Api<ComposCluster>,
    name: &str,
    status: &ComposClusterStatus,
) -> anyhow::Result<()> {
    api.patch_status(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({"status": status})),
    )
    .await?;
    Ok(())
}

fn apply_params() -> PatchParams {
    PatchParams::apply("compos-operator").force()
}

fn default_env_secret() -> String {
    "compos-env".to_string()
}
fn default_cluster_id() -> String {
    "compos".to_string()
}
fn default_object_provider() -> String {
    "s3".to_string()
}
fn default_object_endpoint() -> String {
    "rustfs:9000".to_string()
}
fn default_object_bucket() -> String {
    "compos-rhiza".to_string()
}
fn default_object_prefix() -> String {
    "compos".to_string()
}
fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}
const fn default_replicas() -> i32 {
    3
}
const fn default_server_port() -> i32 {
    7878
}
const fn default_peer_port() -> i32 {
    8100
}
const fn default_true() -> bool {
    true
}

fn log(message: &str) {
    eprintln!("compos-operator {message}");
}

// ─────────────────────────────────────────────────────────────
// ComposSearchSyncer CRD
// ─────────────────────────────────────────────────────────────

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "compos.mrchypark.dev",
    version = "v1alpha1",
    kind = "ComposSearchSyncer",
    plural = "searchsyncers",
    namespaced,
    status = "ComposSearchSyncerStatus",
    shortname = "css"
)]
struct ComposSearchSyncerSpec {
    #[serde(default)]
    meilisearch: MeilisearchSpec,
    syncer: SyncerSpec,
    compos: ComposConnectionSpec,
    #[serde(default)]
    index: IndexSpec,
    #[serde(default)]
    sync: SyncSpec,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncerSpec {
    image: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct MeilisearchSpec {
    #[serde(default = "default_meilisearch_image")]
    image: String,
    #[serde(default = "default_one")]
    replicas: i32,
    #[serde(default = "default_meilisearch_ephemeral_storage")]
    ephemeral_storage: String,
    #[serde(rename = "masterKeySecretRef")]
    master_key_secret_ref: Option<SecretKeyRef>,
    #[serde(default)]
    resources: Option<Value>,
}

impl Default for MeilisearchSpec {
    fn default() -> Self {
        Self {
            image: default_meilisearch_image(),
            replicas: 1,
            ephemeral_storage: default_meilisearch_ephemeral_storage(),
            master_key_secret_ref: None,
            resources: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct SecretKeyRef {
    name: String,
    key: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposConnectionSpec {
    endpoint: String,
    workspace_id: String,
    #[serde(rename = "authSecretRef")]
    auth_secret_ref: Option<SecretKeyRef>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexSpec {
    #[serde(default = "default_index_uid")]
    uid: String,
    #[serde(default = "default_primary_key")]
    primary_key: String,
}

impl Default for IndexSpec {
    fn default() -> Self {
        Self {
            uid: default_index_uid(),
            primary_key: default_primary_key(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncSpec {
    #[serde(default = "default_sync_interval")]
    interval_seconds: u64,
    #[serde(default = "default_batch_size")]
    batch_size: u32,
}

impl Default for SyncSpec {
    fn default() -> Self {
        Self {
            interval_seconds: default_sync_interval(),
            batch_size: default_batch_size(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
struct ComposSearchSyncerStatus {
    phase: Option<String>,
    meilisearch_ready: Option<bool>,
    syncer_ready: Option<bool>,
    last_error: Option<String>,
    observed_generation: Option<i64>,
}

impl Default for ComposSearchSyncerStatus {
    fn default() -> Self {
        Self {
            phase: Some("Pending".to_string()),
            meilisearch_ready: Some(false),
            syncer_ready: Some(false),
            last_error: None,
            observed_generation: None,
        }
    }
}

fn default_meilisearch_image() -> String {
    "meilisearch/meilisearch:v1.12".to_string()
}

const fn default_one() -> i32 {
    1
}

fn default_meilisearch_ephemeral_storage() -> String {
    "512Mi".to_string()
}

fn default_index_uid() -> String {
    "compos-objects".to_string()
}

fn default_primary_key() -> String {
    "id".to_string()
}

const fn default_sync_interval() -> u64 {
    5
}

const fn default_batch_size() -> u32 {
    100
}

fn search_syncer_labels(name: &str) -> Value {
    json!({
        "app.kubernetes.io/name": "compos-search-syncer",
        "app.kubernetes.io/instance": name,
        "app.kubernetes.io/component": "search"
    })
}

fn managed_master_key_ref(name: &str) -> SecretKeyRef {
    SecretKeyRef {
        name: format!("{name}-meilisearch"),
        key: "MASTER_KEY".to_string(),
    }
}

fn master_key_ref(name: &str, spec: &ComposSearchSyncerSpec) -> SecretKeyRef {
    spec.meilisearch
        .master_key_secret_ref
        .clone()
        .unwrap_or_else(|| managed_master_key_ref(name))
}

fn uses_managed_master_key_secret(spec: &ComposSearchSyncerSpec) -> bool {
    spec.meilisearch.master_key_secret_ref.is_none()
}

fn validate_master_key_secret(
    secret: &Secret,
    master_key: &SecretKeyRef,
    owner_uid: Option<&str>,
) -> anyhow::Result<()> {
    if secret.immutable != Some(true) {
        bail!("master-key Secret {} must be immutable", master_key.name);
    }
    if !secret
        .data
        .as_ref()
        .is_some_and(|data| data.contains_key(&master_key.key))
    {
        bail!(
            "master-key Secret {} must contain key {}",
            master_key.name,
            master_key.key
        );
    }
    if let Some(owner_uid) = owner_uid
        && !secret
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|owners| owners.iter().any(|owner| owner.uid == owner_uid))
    {
        bail!(
            "managed master-key Secret {} is not owned by this ComposSearchSyncer",
            master_key.name
        );
    }
    Ok(())
}

fn owner_references(syncer: &ComposSearchSyncer) -> Value {
    serde_json::to_value([syncer.owner_ref(&()).expect("CR owner reference")])
        .expect("owner reference serializes")
}

fn meilisearch_deployment(
    syncer: &ComposSearchSyncer,
    spec: &ComposSearchSyncerSpec,
    master_key: &SecretKeyRef,
) -> Value {
    let name = syncer.name_any();
    let ms = &spec.meilisearch;
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{name}-meilisearch"),
            "labels": search_syncer_labels(&name),
            "ownerReferences": owner_references(syncer)
        },
        "spec": {
            "replicas": ms.replicas,
            "selector": {"matchLabels": {"app.kubernetes.io/instance": &name, "app.kubernetes.io/component": "meilisearch"}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/instance": &name, "app.kubernetes.io/component": "meilisearch"}},
                "spec": {
                    "containers": [{
                        "name": "meilisearch",
                        "image": ms.image,
                        "ports": [{"name": "http", "containerPort": 7700}],
                        "env": [
                            {"name": "MEILI_ENV", "value": "production"},
                            {"name": "MEILI_NO_ANALYTICS", "value": "true"},
                            {"name": "MEILI_MASTER_KEY", "valueFrom": {"secretKeyRef": {"name": &master_key.name, "key": &master_key.key}}}
                        ],
                        "readinessProbe": {
                            "httpGet": {"path": "/health", "port": "http"},
                            "initialDelaySeconds": 5,
                            "periodSeconds": 5
                        },
                        "resources": ms.resources.clone().unwrap_or(json!({})),
                        "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}]
                    }],
                    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": &ms.ephemeral_storage}}]
                }
            }
        }
    })
}

fn meilisearch_service(syncer: &ComposSearchSyncer) -> Value {
    let name = syncer.name_any();
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": format!("{name}-meilisearch"),
            "labels": search_syncer_labels(&name),
            "ownerReferences": owner_references(syncer)
        },
        "spec": {
            "selector": {"app.kubernetes.io/instance": &name, "app.kubernetes.io/component": "meilisearch"},
            "ports": [{"name": "http", "port": 7700, "targetPort": "http"}]
        }
    })
}

fn syncer_deployment(
    syncer: &ComposSearchSyncer,
    spec: &ComposSearchSyncerSpec,
    master_key: &SecretKeyRef,
) -> Value {
    let name = syncer.name_any();
    let compos_env = vec![
        json!({"name": "COMPOS_ENDPOINT", "value": spec.compos.endpoint}),
        json!({"name": "COMPOS_WORKSPACE_ID", "value": spec.compos.workspace_id}),
        json!({"name": "MEILISEARCH_URL", "value": format!("http://{name}-meilisearch:7700")}),
        json!({"name": "MEILISEARCH_INDEX_UID", "value": spec.index.uid}),
        json!({"name": "MEILISEARCH_PRIMARY_KEY", "value": spec.index.primary_key}),
        json!({"name": "SYNC_INTERVAL_SECONDS", "value": spec.sync.interval_seconds.to_string()}),
        json!({"name": "SYNC_BATCH_SIZE", "value": spec.sync.batch_size.to_string()}),
    ];
    let mut env = compos_env;
    if let Some(auth) = &spec.compos.auth_secret_ref {
        env.push(json!({"name": "COMPOS_API_TOKEN", "valueFrom": {"secretKeyRef": {"name": &auth.name, "key": &auth.key}}}));
    }
    env.push(json!({"name": "MEILISEARCH_API_KEY", "valueFrom": {"secretKeyRef": {"name": &master_key.name, "key": &master_key.key}}}));
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": format!("{name}-syncer"),
            "labels": search_syncer_labels(&name),
            "ownerReferences": owner_references(syncer)
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/instance": &name, "app.kubernetes.io/component": "syncer"}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/instance": &name, "app.kubernetes.io/component": "syncer"}},
                "spec": {
                    "containers": [{
                        "name": "syncer",
                        "image": spec.syncer.image,
                        "env": env,
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "200m", "memory": "128Mi"}}
                    }]
                }
            }
        }
    })
}

fn managed_master_key_secret(
    syncer: &ComposSearchSyncer,
    master_key: &SecretKeyRef,
) -> anyhow::Result<Value> {
    let name = syncer.name_any();
    Ok(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": &master_key.name,
            "labels": search_syncer_labels(&name),
            "ownerReferences": owner_references(syncer)
        },
        "type": "Opaque",
        "immutable": true,
        "stringData": {
            &master_key.key: generate_meilisearch_master_key()?
        }
    }))
}

async fn ensure_search_syncer_resources(
    syncer: &ComposSearchSyncer,
    spec: &ComposSearchSyncerSpec,
    deployments: &Api<Deployment>,
    services: &Api<Service>,
    secrets: &Api<Secret>,
) -> anyhow::Result<()> {
    let name = syncer.name_any();
    let master_key = master_key_ref(&name, spec);
    if uses_managed_master_key_secret(spec) && secrets.get_opt(&master_key.name).await?.is_none() {
        apply_secret(secrets, &managed_master_key_secret(syncer, &master_key)?).await?;
    }
    apply_deployment(
        deployments,
        &meilisearch_deployment(syncer, spec, &master_key),
    )
    .await?;
    apply_service(services, &meilisearch_service(syncer)).await?;
    apply_deployment(deployments, &syncer_deployment(syncer, spec, &master_key)).await?;
    Ok(())
}

async fn validate_search_syncer_master_key_secret(
    syncer: &ComposSearchSyncer,
    spec: &ComposSearchSyncerSpec,
    secrets: &Api<Secret>,
) -> anyhow::Result<()> {
    let master_key = master_key_ref(&syncer.name_any(), spec);
    let existing = secrets.get_opt(&master_key.name).await?;
    match (uses_managed_master_key_secret(spec), existing) {
        (true, None) => Ok(()),
        (true, Some(secret)) => {
            let uid = syncer
                .metadata
                .uid
                .as_deref()
                .ok_or_else(|| anyhow!("ComposSearchSyncer must have a UID"))?;
            validate_master_key_secret(&secret, &master_key, Some(uid))
        }
        (false, Some(secret)) => validate_master_key_secret(&secret, &master_key, None),
        (false, None) => bail!(
            "configured master-key Secret {} does not exist",
            master_key.name
        ),
    }
}

fn generate_meilisearch_master_key() -> anyhow::Result<String> {
    let mut key = [0_u8; 32];
    getrandom::fill(&mut key).map_err(|err| anyhow!(err.to_string()))?;
    Ok(key.iter().map(|byte| format!("{byte:02x}")).collect())
}

async fn apply_secret(api: &Api<Secret>, value: &Value) -> anyhow::Result<()> {
    let name = value
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap();
    api.patch(name, &apply_params(), &Patch::Apply(value))
        .await?;
    Ok(())
}

async fn reconcile_search_syncer(
    syncer: Arc<ComposSearchSyncer>,
    ctx: Arc<Context>,
) -> Result<Action, OperatorError> {
    reconcile_search_syncer_inner(syncer, ctx)
        .await
        .map_err(|err| {
            log(&format!("search syncer reconcile error: {err:#}"));
            err.into()
        })
}

async fn reconcile_search_syncer_inner(
    syncer: Arc<ComposSearchSyncer>,
    ctx: Arc<Context>,
) -> anyhow::Result<Action> {
    let namespace = syncer
        .namespace()
        .ok_or_else(|| anyhow!("ComposSearchSyncer must be namespaced"))?;
    let name = syncer.name_any();
    let spec = &syncer.spec;
    let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);
    let syncers: Api<ComposSearchSyncer> = Api::namespaced(ctx.client.clone(), &namespace);

    if let Err(err) = validate_search_syncer_spec(spec) {
        patch_invalid_search_syncer_status(&syncers, &name, &syncer, &err).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }
    if let Err(err) = validate_search_syncer_master_key_secret(&syncer, spec, &secrets).await {
        patch_invalid_search_syncer_status(&syncers, &name, &syncer, &err).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    ensure_search_syncer_resources(&syncer, spec, &deployments, &services, &secrets).await?;

    let mut status = syncer.status.clone().unwrap_or_default();
    status.observed_generation = syncer.metadata.generation;

    let ms_name = format!("{name}-meilisearch");
    let ms_ready = deployment_ready_replicas(&deployments, &ms_name).await? > 0;
    status.meilisearch_ready = Some(ms_ready);

    let syncer_name = format!("{name}-syncer");
    let syncer_ready = deployment_ready_replicas(&deployments, &syncer_name).await? > 0;
    status.syncer_ready = Some(syncer_ready);

    if ms_ready && syncer_ready {
        status.phase = Some("Running".to_string());
        status.last_error = None;
    } else if ms_ready {
        status.phase = Some("SyncerPending".to_string());
    } else {
        status.phase = Some("Deploying".to_string());
    }

    patch_search_syncer_status(&syncers, &name, &status).await?;
    Ok(Action::requeue(Duration::from_secs(30)))
}

async fn patch_invalid_search_syncer_status(
    api: &Api<ComposSearchSyncer>,
    name: &str,
    syncer: &ComposSearchSyncer,
    err: &anyhow::Error,
) -> anyhow::Result<()> {
    patch_search_syncer_status(api, name, &invalid_search_syncer_status(syncer, err)).await
}

fn invalid_search_syncer_status(
    syncer: &ComposSearchSyncer,
    err: &anyhow::Error,
) -> ComposSearchSyncerStatus {
    ComposSearchSyncerStatus {
        phase: Some("Invalid".to_string()),
        last_error: Some(err.to_string()),
        observed_generation: syncer.metadata.generation,
        ..syncer.status.clone().unwrap_or_default()
    }
}

fn validate_search_syncer_spec(spec: &ComposSearchSyncerSpec) -> anyhow::Result<()> {
    if spec.compos.endpoint.trim().is_empty() || spec.compos.workspace_id.trim().is_empty() {
        bail!("spec.compos.endpoint and workspaceId are required");
    }
    if spec.meilisearch.replicas != 1 {
        bail!("spec.meilisearch.replicas must be exactly 1");
    }
    if spec.sync.interval_seconds < 1 || spec.sync.batch_size < 1 {
        bail!("sync interval and batch size must be positive");
    }
    if spec.meilisearch.image.trim().is_empty() || spec.syncer.image.trim().is_empty() {
        bail!("Meilisearch and syncer images are required");
    }
    if let Some(master_key) = &spec.meilisearch.master_key_secret_ref
        && (master_key.name.trim().is_empty() || master_key.key.trim().is_empty())
    {
        bail!("spec.meilisearch.masterKeySecretRef name and key are required when set");
    }
    Ok(())
}

async fn deployment_ready_replicas(
    deployments: &Api<Deployment>,
    name: &str,
) -> anyhow::Result<i32> {
    match deployments.get_opt(name).await? {
        Some(deploy) => Ok(deploy.status.and_then(|s| s.ready_replicas).unwrap_or(0)),
        None => Ok(0),
    }
}

async fn patch_search_syncer_status(
    api: &Api<ComposSearchSyncer>,
    name: &str,
    status: &ComposSearchSyncerStatus,
) -> anyhow::Result<()> {
    api.patch_status(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({"status": status})),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
