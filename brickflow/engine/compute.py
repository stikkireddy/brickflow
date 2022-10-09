import dataclasses
from dataclasses import dataclass
from typing import Optional, Dict, Any, List


class DuplicateClustersDefinitionError(Exception):
    pass


# Not an enum but collection of constants, generated via script
class Runtimes:
    RUNTIME_11_3_X_SCALA2_12 = "11.3.x-scala2.12"
    RUNTIME_11_3_X_PHOTON_SCALA2_12 = "11.3.x-photon-scala2.12"
    RUNTIME_11_3_X_GPU_ML_SCALA2_12 = "11.3.x-gpu-ml-scala2.12"
    RUNTIME_11_3_X_CPU_ML_SCALA2_12 = "11.3.x-cpu-ml-scala2.12"
    RUNTIME_11_3_X_AARCH64_SCALA2_12 = "11.3.x-aarch64-scala2.12"
    RUNTIME_11_3_X_AARCH64_PHOTON_SCALA2_12 = "11.3.x-aarch64-photon-scala2.12"
    RUNTIME_11_2_X_SCALA2_12 = "11.2.x-scala2.12"
    RUNTIME_11_2_X_PHOTON_SCALA2_12 = "11.2.x-photon-scala2.12"
    RUNTIME_11_2_X_GPU_ML_SCALA2_12 = "11.2.x-gpu-ml-scala2.12"
    RUNTIME_11_2_X_CPU_ML_SCALA2_12 = "11.2.x-cpu-ml-scala2.12"
    RUNTIME_11_2_X_AARCH64_SCALA2_12 = "11.2.x-aarch64-scala2.12"
    RUNTIME_11_2_X_AARCH64_PHOTON_SCALA2_12 = "11.2.x-aarch64-photon-scala2.12"
    RUNTIME_11_1_X_SCALA2_12 = "11.1.x-scala2.12"
    RUNTIME_11_1_X_PHOTON_SCALA2_12 = "11.1.x-photon-scala2.12"
    RUNTIME_11_1_X_GPU_ML_SCALA2_12 = "11.1.x-gpu-ml-scala2.12"
    RUNTIME_11_1_X_CPU_ML_SCALA2_12 = "11.1.x-cpu-ml-scala2.12"
    RUNTIME_11_1_X_AARCH64_SCALA2_12 = "11.1.x-aarch64-scala2.12"
    RUNTIME_11_1_X_AARCH64_PHOTON_SCALA2_12 = "11.1.x-aarch64-photon-scala2.12"
    RUNTIME_11_0_X_SCALA2_12 = "11.0.x-scala2.12"
    RUNTIME_11_0_X_PHOTON_SCALA2_12 = "11.0.x-photon-scala2.12"
    RUNTIME_11_0_X_GPU_ML_SCALA2_12 = "11.0.x-gpu-ml-scala2.12"
    RUNTIME_11_0_X_CPU_ML_SCALA2_12 = "11.0.x-cpu-ml-scala2.12"
    RUNTIME_11_0_X_AARCH64_SCALA2_12 = "11.0.x-aarch64-scala2.12"
    RUNTIME_11_0_X_AARCH64_PHOTON_SCALA2_12 = "11.0.x-aarch64-photon-scala2.12"
    RUNTIME_10_5_X_SCALA2_12 = "10.5.x-scala2.12"
    RUNTIME_10_5_X_PHOTON_SCALA2_12 = "10.5.x-photon-scala2.12"
    RUNTIME_10_5_X_GPU_ML_SCALA2_12 = "10.5.x-gpu-ml-scala2.12"
    RUNTIME_10_5_X_CPU_ML_SCALA2_12 = "10.5.x-cpu-ml-scala2.12"
    RUNTIME_10_5_X_AARCH64_SCALA2_12 = "10.5.x-aarch64-scala2.12"
    RUNTIME_10_5_X_AARCH64_PHOTON_SCALA2_12 = "10.5.x-aarch64-photon-scala2.12"
    RUNTIME_10_4_X_SCALA2_12_LTS = "10.4.x-scala2.12"
    RUNTIME_10_4_X_PHOTON_SCALA2_12_LTS = "10.4.x-photon-scala2.12"
    RUNTIME_10_4_X_GPU_ML_SCALA2_12_LTS = "10.4.x-gpu-ml-scala2.12"
    RUNTIME_10_4_X_CPU_ML_SCALA2_12_LTS = "10.4.x-cpu-ml-scala2.12"
    RUNTIME_10_4_X_AARCH64_SCALA2_12_LTS = "10.4.x-aarch64-scala2.12"
    RUNTIME_10_4_X_AARCH64_PHOTON_SCALA2_12_LTS = "10.4.x-aarch64-photon-scala2.12"
    RUNTIME_9_1_X_SCALA2_12_LTS = "9.1.x-scala2.12"
    RUNTIME_9_1_X_PHOTON_SCALA2_12_LTS = "9.1.x-photon-scala2.12"
    RUNTIME_9_1_X_GPU_ML_SCALA2_12_LTS = "9.1.x-gpu-ml-scala2.12"
    RUNTIME_9_1_X_CPU_ML_SCALA2_12_LTS = "9.1.x-cpu-ml-scala2.12"
    RUNTIME_9_1_X_AARCH64_SCALA2_12_LTS = "9.1.x-aarch64-scala2.12"
    RUNTIME_7_3_X_SCALA2_12_LTS = "7.3.x-scala2.12"
    RUNTIME_7_3_X_HLS_SCALA2_12_LTS = "7.3.x-hls-scala2.12"
    RUNTIME_7_3_X_GPU_ML_SCALA2_12_LTS = "7.3.x-gpu-ml-scala2.12"
    RUNTIME_7_3_X_CPU_ML_SCALA2_12_LTS = "7.3.x-cpu-ml-scala2.12"


@dataclass(frozen=True)
class Cluster:
    name: str
    spark_version: str
    existing_cluster_id: Optional[str] = None
    num_workers: Optional[int] = None
    min_workers: Optional[int] = None
    max_workers: Optional[int] = None
    spark_conf: Optional[Dict[str, str]] = None
    aws_attributes: Optional[Dict[str, Any]] = None
    node_type_id: Optional[str] = None
    driver_node_type_id: Optional[str] = None
    custom_tags: Optional[Dict[str, str]] = None
    init_scripts: Optional[List[Dict[str, str]]] = None
    spark_env_vars: Optional[Dict[str, str]] = None
    enable_elastic_disk: Optional[bool] = None
    driver_instance_pool_id: Optional[str] = None
    instance_pool_id: Optional[str] = None
    policy_id: Optional[str] = None

    @classmethod
    def from_existing_cluster(cls, existing_cluster_id: str) -> "Cluster":
        return Cluster(
            existing_cluster_id,
            existing_cluster_id,
            existing_cluster_id=existing_cluster_id,
        )

    @property
    def is_new_job_cluster(self) -> bool:
        return self.existing_cluster_id is None

    @property
    def autoscale(self) -> Dict[str, Any]:
        if self.min_workers is not None and self.max_workers is not None:
            return {
                "autoscale": {
                    "min_workers": self.min_workers,
                    "max_workers": self.max_workers,
                }
            }
        return {}

    @property
    def job_task_field_dict(self) -> Dict[str, str]:
        if self.existing_cluster_id is not None:
            return {"existing_cluster_id": self.existing_cluster_id}
        return {"job_cluster_key": self.name}

    @staticmethod
    def cleanup(d: Dict[str, Any]) -> None:
        d.pop("min_workers", None)
        d.pop("max_workers", None)
        d.pop("existing_cluster_id", None)

    @property
    def dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        d = {**d, **self.autoscale}
        self.cleanup(d)
        return d
