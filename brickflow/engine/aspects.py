import os
from typing import Optional

import constructs
import jsii
from cdktf import IAspect
from brickflow.engine import BrickflowEnvVars
from brickflow.tf.databricks import Job, DataDatabricksCurrentUser


@jsii.implements(IAspect)
class LocalModeVisitor:
    def __init__(self) -> None:
        self._current_user: Optional[DataDatabricksCurrentUser] = None

    def visit(self, node: constructs.IConstruct) -> None:
        # pass
        if isinstance(node, Job):
            if (
                os.environ.get(BrickflowEnvVars.BRICKFLOW_LOCAL_MODE.value, "false")
                == "true"
            ):
                if self._current_user is None:
                    self._current_user = DataDatabricksCurrentUser(
                        node.cdktf_stack, "current_user"
                    )
                node.name = f"{self._current_user.alphanumeric}_{node.name_input}"
