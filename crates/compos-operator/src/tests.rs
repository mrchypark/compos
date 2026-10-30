use super::*;
use kube::CustomResourceExt;

const CHART_CLUSTER_CRD: &str =
    include_str!("../../../deploy/charts/operator/crds/clusters.compos.mrchypark.dev.yaml");
const CHART_SEARCH_SYNCER_CRD: &str =
    include_str!("../../../deploy/charts/operator/crds/searchsyncers.compos.mrchypark.dev.yaml");

fn spec() -> ComposClusterSpec {
    ComposClusterSpec {
        server: ServerSpec {
            env_secret: default_env_secret(),
            image: "registry.example/compos-server:sha".to_string(),
            port: default_server_port(),
            rhiza: RhizaSpec::default(),
        },
        object_store: ObjectStoreSpec::default(),
        replicas: 3,
        image_pull_policy: default_image_pull_policy(),
    }
}

fn cluster() -> ComposCluster {
    let mut cluster = ComposCluster::new("compos", spec());
    cluster.metadata.uid = Some("cluster-uid".to_string());
    cluster
}

fn search_syncer(master_key_secret_ref: Option<SecretKeyRef>) -> ComposSearchSyncer {
    let mut syncer = ComposSearchSyncer::new(
        "search",
        ComposSearchSyncerSpec {
            meilisearch: MeilisearchSpec {
                master_key_secret_ref,
                ..Default::default()
            },
            syncer: SyncerSpec {
                image: "registry.example/compos-search-syncer:sha".to_string(),
            },
            compos: ComposConnectionSpec {
                endpoint: "http://compos:7878".to_string(),
                workspace_id: "wks_test".to_string(),
                auth_secret_ref: None,
            },
            index: IndexSpec::default(),
            sync: SyncSpec::default(),
        },
    );
    syncer.metadata.uid = Some("search-uid".to_string());
    syncer
}

fn assert_owned_by(syncer: &ComposSearchSyncer, resource: &Value) {
    assert_eq!(
        resource["metadata"]["ownerReferences"][0]["uid"],
        syncer.metadata.uid.as_deref().unwrap()
    );
}

fn existing_master_key_secret(uid: &str, immutable: bool) -> Secret {
    serde_json::from_value(json!({
        "metadata": {"name": "search-meilisearch", "ownerReferences": [{"apiVersion": "compos.mrchypark.dev/v1alpha1", "kind": "ComposSearchSyncer", "name": "search", "uid": uid}]},
        "immutable": immutable,
        "data": {"MASTER_KEY": "dGVzdA=="}
    }))
    .unwrap()
}

#[test]
fn cluster_crd_exposes_only_rhiza_storage_contract() {
    let crd = serde_json::to_value(ComposCluster::crd()).unwrap();
    let schema = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"];
    assert_eq!(
        schema["properties"]["spec"]["properties"]["replicas"]["default"],
        3
    );
    let server_schema = &schema["properties"]["spec"]["properties"]["server"];
    let server = &server_schema["properties"];
    assert!(server.get("rhiza").is_some());
    assert!(server.get("quorum").is_none());
    assert!(server.get("backup").is_none());
    assert!(server.get("dr").is_none());
    assert!(
        server_schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("image"))
    );
    let chart_crd: Value = serde_json::from_str(CHART_CLUSTER_CRD).unwrap();
    assert_eq!(chart_crd, crd);
}

#[test]
fn rhiza_cluster_requires_odd_three_or_more_members() {
    let mut value = spec();
    value.replicas = 1;
    assert!(validate_spec(&value).is_err());
    value.replicas = 4;
    assert!(validate_spec(&value).is_err());
    value.replicas = 3;
    assert!(validate_spec(&value).is_ok());
}

#[test]
fn statefulset_uses_parallel_members_and_remote_durable_ephemeral_storage() {
    let value = spec();
    let cluster = cluster();
    let set = compos_statefulset(&cluster, &value);
    let pod = &set["spec"]["template"]["spec"];
    let container = &pod["containers"][0];
    let rendered = container.to_string();

    assert!(rendered.contains("COMPOS_RHIZA_MEMBERS"));
    assert!(rendered.contains("COMPOS_RHIZA_OBJECT_DURABILITY"));
    assert!(rendered.contains("before-ack"));
    assert!(rendered.contains("COMPOS_RHIZA_ADMIN_TOKEN"));
    assert_eq!(container["ports"][1]["protocol"], "UDP");
    assert_eq!(container["envFrom"][0]["secretRef"]["name"], "compos-env");
    assert_eq!(set["spec"]["podManagementPolicy"], "Parallel");
    assert_eq!(pod["volumes"][0], json!({"name": "data", "emptyDir": {}}));
    assert!(set["spec"].get("volumeClaimTemplates").is_none());

    let members: Value = serde_json::from_str(&rhiza_members("compos", &value)).unwrap();
    assert_eq!(members.as_array().unwrap().len(), 3);
    assert_eq!(members[2]["node_id"], "compos-2");
    assert_eq!(
        members[2]["peer_url"],
        "quic://compos-2.compos-headless:8100"
    );
    assert!(members[2].get("token").is_none());
    assert!(members[2].get("public_key").is_none());

    for resource in [
        headless_service(&cluster, &value),
        http_service(&cluster, &value),
        set,
    ] {
        assert_eq!(
            resource["metadata"]["ownerReferences"][0]["uid"],
            "cluster-uid"
        );
    }
}

#[test]
fn invalid_cluster_status_is_observable() {
    let cluster = cluster();
    let status = invalid_cluster_status(&cluster, &anyhow!("spec.server.image is required"));
    assert_eq!(status.phase.as_deref(), Some("Invalid"));
    assert_eq!(
        status.last_error.as_deref(),
        Some("spec.server.image is required")
    );
}

#[test]
fn search_syncer_defaults_remain_stable() {
    assert_eq!(default_sync_interval(), 5);
    assert_eq!(default_batch_size(), 100);
    assert_eq!(default_index_uid(), "compos-objects");

    let generated = generate_meilisearch_master_key().unwrap();
    assert_eq!(generated.len(), 64);
    assert_ne!(generated, generate_meilisearch_master_key().unwrap());

    let crd = serde_json::to_value(ComposSearchSyncer::crd()).unwrap();
    assert_eq!(crd["metadata"]["name"], "searchsyncers.compos.mrchypark.dev");
    let chart_crd: Value = serde_json::from_str(CHART_SEARCH_SYNCER_CRD).unwrap();
    assert_eq!(chart_crd, crd);

    let mut invalid = search_syncer(None).spec;
    invalid.meilisearch.replicas = 2;
    assert!(validate_search_syncer_spec(&invalid).is_err());
}

#[test]
fn search_syncer_owns_its_generated_resources_and_not_explicit_secrets() {
    let managed = search_syncer(None);
    let managed_key = master_key_ref(&managed.name_any(), &managed.spec);
    assert_eq!(managed_key.name, "search-meilisearch");
    assert_eq!(managed_key.key, "MASTER_KEY");
    assert!(uses_managed_master_key_secret(&managed.spec));

    assert_owned_by(
        &managed,
        &meilisearch_deployment(&managed, &managed.spec, &managed_key),
    );
    assert_owned_by(&managed, &meilisearch_service(&managed));
    assert_owned_by(
        &managed,
        &syncer_deployment(&managed, &managed.spec, &managed_key),
    );
    assert_owned_by(
        &managed,
        &managed_master_key_secret(&managed, &managed_key).unwrap(),
    );

    let explicit_ref = SecretKeyRef {
        name: "operator-managed-elsewhere".to_string(),
        key: "MEILI_KEY".to_string(),
    };
    let explicit = search_syncer(Some(explicit_ref.clone()));
    let explicit_key = master_key_ref(&explicit.name_any(), &explicit.spec);
    assert_eq!(explicit_key.name, explicit_ref.name);
    assert_eq!(explicit_key.key, explicit_ref.key);
    assert!(!uses_managed_master_key_secret(&explicit.spec));
}

#[test]
fn existing_master_key_secret_must_be_immutable_and_owned_when_managed() {
    let managed = search_syncer(None);
    let master_key = master_key_ref(&managed.name_any(), &managed.spec);
    let uid = managed.metadata.uid.as_deref().unwrap();
    assert!(
        validate_master_key_secret(
            &existing_master_key_secret(uid, true),
            &master_key,
            Some(uid),
        )
        .is_ok()
    );
    assert!(
        validate_master_key_secret(
            &existing_master_key_secret("other-uid", true),
            &master_key,
            Some(uid),
        )
        .is_err()
    );
    assert!(
        validate_master_key_secret(
            &existing_master_key_secret(uid, false),
            &master_key,
            Some(uid),
        )
        .is_err()
    );

    let explicit = search_syncer(Some(SecretKeyRef {
        name: "search-meilisearch".to_string(),
        key: "MASTER_KEY".to_string(),
    }));
    assert!(
        validate_master_key_secret(
            &existing_master_key_secret("other-uid", true),
            &master_key_ref(&explicit.name_any(), &explicit.spec),
            None,
        )
        .is_ok()
    );
}

#[test]
fn invalid_search_syncer_status_is_observable_without_child_creation() {
    let syncer = search_syncer(None);
    let status = invalid_search_syncer_status(&syncer, &anyhow!("configured Secret is missing"));
    assert_eq!(status.phase.as_deref(), Some("Invalid"));
    assert_eq!(
        status.last_error.as_deref(),
        Some("configured Secret is missing")
    );
    assert_eq!(status.observed_generation, syncer.metadata.generation);
}
