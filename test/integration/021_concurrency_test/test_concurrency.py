from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest


class TestConcurrency(DBTIntegrationTest):

    def setUp(self):
        pass

    @property
    def schema(self):
        return "concurrency_021"

    @property
    def models(self):
        return "test/integration/021_concurrency_test/models"

    @attr(type='postgres')
    def test__postgres__concurrency(self):
        self.use_profile('postgres')
        self.use_default_project()
        self.run_sql_file("test/integration/021_concurrency_test/seed.sql")

        self.run_dbt(expect_pass=False)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

        self.run_sql_file("test/integration/021_concurrency_test/update.sql")

        self.run_dbt(expect_pass=False)

        self.assertTablesEqual("seed", "view_model")
        self.assertTablesEqual("seed", "dep")
        self.assertTablesEqual("seed", "table_a")
        self.assertTablesEqual("seed", "table_b")
        self.assertTableDoesNotExist("invalid")
        self.assertTableDoesNotExist("skip")

    @attr(type='snowflake')
    def test__snowflake__concurrency(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/021_concurrency_test/seed.sql")

        self.run_dbt(expect_pass=False)

        self.assertManyTablesEqual({
            "SEED": [
                "view_model",
                "dep",
                "table_a",
                "table_b"
            ]
        })

        self.run_sql_file("test/integration/021_concurrency_test/update.sql")

        self.run_dbt(expect_pass=False)

        self.assertManyTablesEqual({
            "SEED": [
                "view_model",
                "dep",
                "table_a",
                "table_b",
            ]
        })
