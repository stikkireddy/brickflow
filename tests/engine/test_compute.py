from brickflow.engine.compute import Cluster


class TestCompute:
    def test_autoscale(self):
        workers = 1234
        cluster = Cluster(
            "name", "spark_version", min_workers=workers, max_workers=workers
        )
        assert cluster.autoscale == {
            "autoscale": {
                "min_workers": workers,
                "max_workers": workers,
            }
        }

        cluster = Cluster("name", "spark_version")
        assert not cluster.autoscale

    def test_job_task_field(self):
        cluster = Cluster.from_existing_cluster("existing_cluster_id")
        assert cluster.job_task_field_dict == {
            "existing_cluster_id": "existing_cluster_id"
        }
        cluster = Cluster("name", "spark_version")
        assert cluster.job_task_field_dict == {"job_cluster_key": "name"}

    def test_dict(self):
        cluster = Cluster.from_existing_cluster("existing_cluster_id")
        assert "existing_cluster_id" not in cluster.dict
