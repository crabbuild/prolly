use {
    async_trait::async_trait, gluesql_core::prelude::Glue, gluesql_test_suite::*,
    prolly_gluesql::ProllyStorage,
};

struct ProllyTester {
    glue: Glue<ProllyStorage<prolly::MemStore>>,
}

#[async_trait(?Send)]
impl Tester<ProllyStorage<prolly::MemStore>> for ProllyTester {
    async fn new(_namespace: &str) -> Self {
        Self {
            glue: Glue::new(ProllyStorage::in_memory().expect("open in-memory Prolly storage")),
        }
    }

    fn get_glue(&mut self) -> &mut Glue<ProllyStorage<prolly::MemStore>> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, ProllyTester);
generate_alter_table_tests!(tokio::test, ProllyTester);
generate_custom_function_tests!(tokio::test, ProllyTester);
generate_index_tests!(tokio::test, ProllyTester);
generate_transaction_tests!(tokio::test, ProllyTester);
generate_alter_table_index_tests!(tokio::test, ProllyTester);
generate_transaction_alter_table_tests!(tokio::test, ProllyTester);
generate_transaction_index_tests!(tokio::test, ProllyTester);
generate_metadata_table_tests!(tokio::test, ProllyTester);
generate_metadata_index_tests!(tokio::test, ProllyTester);
