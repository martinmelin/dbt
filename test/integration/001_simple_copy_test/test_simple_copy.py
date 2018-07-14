from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestSimpleCopy(DBTIntegrationTest):

    def setUp(self):
        pass

    @property
    def schema(self):
        return "simple_copy_001"

    @staticmethod
    def dir(path):
        return "test/integration/001_simple_copy_test/" + path.lstrip("/")

    @property
    def models(self):
        return self.dir("models")

    @attr(type="postgres")
    def test__postgres__simple_copy(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertTablesEqual("seed","view_model")
        self.assertTablesEqual("seed","incremental")
        self.assertTablesEqual("seed","materialized")

    @attr(type="postgres")
    def test__postgres__dbt_doesnt_run_empty_models(self):
        self.use_profile("postgres")
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        models = self.get_models_in_schema()

        self.assertFalse("empty" in models.keys())
        self.assertFalse("disabled" in models.keys())

    @attr(type="snowflake")
    def test__snowflake__simple_copy(self):
        self.use_profile("snowflake")
        self.use_default_project({"data-paths": [self.dir("seed-initial")]})

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "seed": [
                "view_model",
                "incremental",
                "materialized"
            ]
        })

        self.use_default_project({"data-paths": [self.dir("seed-update")]})
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "seed": [
                "view_model",
                "incremental",
                "materialized"
            ]
        })

    @attr(type="snowflake")
    def test__snowflake__simple_copy__quoting_on(self):
        self.use_profile("snowflake")
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": True},
        })

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "seed": [
                "view_model",
                "incremental",
                "materialized"
            ]
        })

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": True},
        })
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "seed": [
                "view_model",
                "incremental",
                "materialized"
            ]
        })

    @attr(type="snowflake")
    def test__snowflake__simple_copy__quoting_off(self):
        self.use_profile("snowflake")
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": False},
        })

        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "SEED": [
                "VIEW_MODEL",
                "INCREMENTAL",
                "MATERIALIZED"
            ]
        })

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": False},
        })
        self.run_dbt(["seed"])
        self.run_dbt()

        self.assertManyTablesEqual({
            "SEED": [
                "VIEW_MODEL",
                "INCREMENTAL",
                "MATERIALIZED"
            ]
        })

    @attr(type="snowflake")
    def test__snowflake__seed__quoting_switch(self):
        self.use_profile("snowflake")
        self.use_default_project({
            "data-paths": [self.dir("seed-initial")],
            "quoting": {"identifier": False},
        })

        self.run_dbt(["seed"])

        self.use_default_project({
            "data-paths": [self.dir("seed-update")],
            "quoting": {"identifier": True},
        })
        self.run_dbt(["seed"], expect_pass=False)
