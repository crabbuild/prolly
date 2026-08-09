import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROVIDER = ROOT / "stores" / "prolly-store-dynamodb" / "src" / "lib.rs"
POLICIES = ROOT / "dynamodb" / "client" / "deploy" / "aws"

SDK_CALLS = {
    "batch_get_item": "dynamodb:BatchGetItem",
    "batch_write_item": "dynamodb:BatchWriteItem",
    "create_table": "dynamodb:CreateTable",
    "delete_item": "dynamodb:DeleteItem",
    "describe_table": "dynamodb:DescribeTable",
    "get_item": "dynamodb:GetItem",
    "put_item": "dynamodb:PutItem",
    "query": "dynamodb:Query",
    "scan": "dynamodb:Scan",
    "transact_write_items": "dynamodb:TransactWriteItems",
}


def actions(policy):
    result = set()
    for statement in policy["Statement"]:
        declared = statement["Action"]
        result.update([declared] if isinstance(declared, str) else declared)
    return result


class SecurityPolicyContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = json.loads(
            (POLICIES / "runtime-policy.json").read_text(encoding="utf-8")
        )
        cls.provisioner = json.loads(
            (POLICIES / "provisioner-policy.json").read_text(encoding="utf-8")
        )

    def test_policy_union_covers_exact_provider_apis(self):
        source = PROVIDER.read_text(encoding="utf-8")
        calls = {
            SDK_CALLS[name]
            for name in SDK_CALLS
            if re.search(rf"\.{name}\(\)", source)
        }
        self.assertEqual(actions(self.runtime) | actions(self.provisioner), calls)

    def test_runtime_excludes_control_plane_and_table_deletion(self):
        runtime = actions(self.runtime)
        self.assertNotIn("dynamodb:CreateTable", runtime)
        self.assertNotIn("dynamodb:UpdateTable", runtime)
        self.assertNotIn("dynamodb:DeleteTable", runtime)
        self.assertNotIn("dynamodb:*", runtime)

    def test_provisioner_has_no_data_plane_or_deletion(self):
        self.assertEqual(
            actions(self.provisioner),
            {"dynamodb:CreateTable", "dynamodb:DescribeTable"},
        )

    def test_every_allow_is_bound_to_both_exact_table_placeholders(self):
        expected = {
            "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/NODE_TABLE",
            "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/ROOT_TABLE",
        }
        for policy in (self.runtime, self.provisioner):
            for statement in policy["Statement"]:
                self.assertEqual(statement["Effect"], "Allow")
                self.assertEqual(set(statement["Resource"]), expected)


if __name__ == "__main__":
    unittest.main()
