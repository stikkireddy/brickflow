from typing import Optional


class Compute:
    def __init__(
        self,
        compute_id: str,
        cluster_min_vcpus: int = 64,
        cluster_max_vcpus: int = 64,
        min_gpus_per_worker: int = 0,
        min_memory_gb_per_worker: int = 32,
        gb_per_core: Optional[int] = None,
        category: str = "Memory Optimized",
        photon: bool = True,
        photon_worker: Optional[bool] = None,
        photon_driver: Optional[bool] = None,
        graviton: bool = True,
        delta_cache: bool = True,
        support_port_forwarding: bool = False,
        lts: bool = True,
        latest: bool = False,
        ml: bool = False,
        genomics: bool = False,
        gpu: bool = False,
        beta: bool = False,
        spark_version: Optional[str] = None,
    ) -> None:
        self.min_gpus_per_worker = min_gpus_per_worker
        self.cluster_max_vcpus = cluster_max_vcpus
        self.min_memory_gb_per_worker = min_memory_gb_per_worker
        self.cluster_min_vcpus = cluster_min_vcpus
        self.cluster_min_vcpus = cluster_min_vcpus
        self.gb_per_core = gb_per_core
        self.compute_id = compute_id
        self.spark_version = spark_version
        self.beta = beta
        self.gpu = True if min_gpus_per_worker > 0 else gpu
        self.genomics = genomics
        self.ml = ml
        self.latest = latest
        self.lts = lts
        self.support_port_forwarding = support_port_forwarding
        self.delta_cache = delta_cache
        self.graviton = graviton
        self.photon_driver = photon or photon_driver
        self.photon_worker = photon or photon_worker
        self.category = category

    # def to_node_tf(self, stack: 'TerraformStack'):
    #     from cdktf import TerraformStack
    #     stack: TerraformStack
    #     return DataDatabricksNodeType(stack,
    #                                   id_=f"{self.compute_id}_node",
    #                                   min_memory_gb=self.min_memory_gb_per_worker,
    #                                   category=self.category,
    #                                   gb_per_core=self.gb_per_core,
    #                                   graviton=self.graviton,
    #                                   is_io_cache_enabled=self.delta_cache,
    #                                   min_gpus=self.min_gpus_per_worker,
    #                                   photon_worker_capable=self.photon_worker,
    #                                   photon_driver_capable=self.photon_driver,
    #                                   support_port_forwarding=self.support_port_forwarding
    #                                   )
    #
    # def to_runtime_tf(self, stack: 'TerraformStack'):
    #     from cdktf import TerraformStack
    #     from brickflow.tf.databricks import DataDatabricksNodeType, DataDatabricksSparkVersion
    #     stack: TerraformStack
    #     return DataDatabricksSparkVersion(stack,
    #                                       id_=f"{self.compute_id}_runtime",
    #                                       latest=self.latest,
    #                                       long_term_support=self.lts,
    #                                       ml=self.ml,
    #                                       genomics=self.genomics,
    #                                       gpu=self.gpu,
    #                                       photon=self.photon_worker,
    #                                       graviton=self.graviton,
    #                                       beta=self.beta,
    #                                       )
    #
    def set_to_default(self) -> None:
        self.compute_id = "default"


# class SelfDefinedCluster:
#     def __init__(
#         self,
#         node_type_id: str,
#         driver_node_type_id: str,
#         min_workers: int,
#         max_workers: int,
#         photon: bool,
#     ):
#         self.photon = photon
#         self.max_workers = max_workers
#         self.min_workers = min_workers
#         self.driver_node_type_id = driver_node_type_id
#         self.node_type_id = node_type_id
