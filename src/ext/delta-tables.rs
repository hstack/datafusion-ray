use crate::ext::Extension;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use deltalake::delta_datafusion::{DeltaPhysicalCodec, DeltaTableFactory};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct DeltaTables {}

impl Default for DeltaTables {
    fn default() -> Self {
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);
        Self {}
    }
}

#[async_trait]
impl Extension for DeltaTables {
    async fn init(
        &self,
        ctx: &SessionContext,
        _settings: &HashMap<String, String>,
    ) -> datafusion::common::Result<()> {
        ctx.state_ref()
            .write()
            .table_factories_mut()
            .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
        Ok(())
    }

    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![Box::new(DeltaPhysicalCodec {})]
    }
}
