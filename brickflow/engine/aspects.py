import os
from typing import Optional, Dict, TYPE_CHECKING

import constructs
import jsii
from cdktf import IAspect

from brickflow import get_brickflow_version
from brickflow.engine import BrickflowEnvVars
from brickflow.tf import DATABRICKS_TERRAFORM_PROVIDER_VERSION
from brickflow.tf.databricks import Job, DataDatabricksCurrentUser

if TYPE_CHECKING:
    from brickflow.engine.project import _Project  # pragma: no cover


BRICKFLOW_DEPLOY_TAGS = {
    "brickflow_version": get_brickflow_version(),
}


def has_tags(node: constructs.IConstruct) -> bool:
    return hasattr(node, "tags") and hasattr(node, "tags_input")


def add_brickflow_tags(node: constructs.IConstruct, other_tags: Dict[str, str]) -> None:
    if has_tags(node) is True:
        node.tags = {**(node.tags_input or {}), **other_tags, **BRICKFLOW_DEPLOY_TAGS}


@jsii.implements(IAspect)
class LocalModeVisitor:
    def __init__(self, project: "_Project") -> None:
        self._project = project
        self._current_user: Optional[DataDatabricksCurrentUser] = None

    def visit(self, node: constructs.IConstruct) -> None:
        # pass
        if (
            os.environ.get(BrickflowEnvVars.BRICKFLOW_LOCAL_MODE.value, "false")
            == "true"
        ):
            if isinstance(node, Job):
                if self._current_user is None:
                    self._current_user = DataDatabricksCurrentUser(
                        node.cdktf_stack, "current_user"
                    )
                node.name = f"{self._current_user.alphanumeric}_{node.name_input}"
                add_brickflow_tags(
                    node,
                    {
                        "deployed_by": self._current_user.user_name,
                        "brickflow_project_name": self._project.name,
                        "databricks_tf_provider_version": DATABRICKS_TERRAFORM_PROVIDER_VERSION,
                    },
                )
