use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

#[path = "ext/built-in.rs"]
mod built_in;

#[async_trait]
pub(crate) trait Extension: Debug + Send + Sync + 'static {
    async fn init(&self, ctx: &SessionContext, settings: &HashMap<String, String>) -> Result<()> {
        let _ = ctx;
        let _ = settings;
        Ok(())
    }
    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![]
    }
}

#[derive(Debug)]
pub(crate) struct Extensions(Box<[Box<dyn Extension>]>);

#[async_trait]
impl Extension for Extensions {
    async fn init(&self, ctx: &SessionContext, settings: &HashMap<String, String>) -> Result<()> {
        for ext in &self.0 {
            ext.init(ctx, settings).await?;
        }
        Ok(())
    }

    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        self.0.iter().flat_map(|ext| ext.codecs()).collect()
    }
}

impl Default for Extensions {
    fn default() -> Self {
        Self(Box::new([
            Box::new(built_in::DefaultExtension::default()),
        ]))
    }
}

impl Extensions {
    fn singleton() -> &'static Self {
        lazy_static! {
            static ref SINGLETON: Extensions = Default::default();
        }
        &SINGLETON
    }

    pub(crate) async fn setup(
        ctx: &SessionContext,
        settings: &HashMap<String, String>,
    ) -> Result<()> {
        Self::singleton().init(ctx, settings).await
    }

    pub(crate) fn codec() -> &'static dyn PhysicalExtensionCodec {
        lazy_static! {
            static ref CODEC: CompositeCodec =
                CompositeCodec(Extensions::singleton().codecs().into());
        }
        &*CODEC
    }
}

#[derive(Debug)]
struct CompositeCodec(Box<[Box<dyn PhysicalExtensionCodec>]>);

impl CompositeCodec {
    fn first_successful<R, F>(&self, mut f: F, error_msg: impl Into<String>) -> Result<R>
    where
        F: FnMut(&dyn PhysicalExtensionCodec) -> Result<R>,
    {
        self.0
            .iter()
            .filter_map(|codec| f(codec.as_ref()).ok())
            .next()
            .ok_or_else(|| DataFusionError::Execution(error_msg.into()))
    }
}

impl PhysicalExtensionCodec for CompositeCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.first_successful(
            |codec| codec.try_decode(buf, inputs, registry),
            "No compatible codec found",
        )
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.first_successful(
            |codec| codec.try_encode(node.clone(), buf),
            format!("No compatible codec found for {}", node.name()),
        )
    }
}
