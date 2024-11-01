use crate::ext::Extension;
use aep_delta::catalog::CatalogConfig;
use aep_delta::catalog_provider::DynamicAepCatalogProvider;
use aep_delta::config::AppConfig;
use aep_delta::ims_credentials::ImsCredentialsProvider;
use aep_delta::AepCodec;
use async_trait::async_trait;
use cobweb::CobwebQueryDriver;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_table_providers::flight::FlightTableFactory;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct AdobeExtension {}

pub const IMS_USER_TOKEN: &str = "adobe-ims-user-token";
pub const IMS_ORG: &str = "adobe-ims-org";
pub const SANDBOX_NAME: &str = "adobe-sandbox-name";

#[async_trait]
impl Extension for AdobeExtension {
    async fn init(
        &self,
        ctx: &SessionContext,
        settings: &HashMap<String, String>,
    ) -> datafusion::common::Result<()> {
        let cfg = AppConfig::new().unwrap();
        let creds_provider = ImsCredentialsProvider::new(cfg.ims.clone(), None);
        let client_id = cfg.ims.client_id;

        let token = creds_provider
            .get_token()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        ctx.state_ref().write().table_factories_mut().insert(
            "COBWEB".to_string(),
            Arc::new(FlightTableFactory::new(Arc::new(CobwebQueryDriver::new(
                HashMap::from([
                    ("ims.token".into(), token.clone().access_token),
                    ("ims.client_id".into(), client_id.clone()),
                ]),
            )))),
        );

        let user_token = settings.get(IMS_USER_TOKEN).unwrap();
        if user_token.clone().trim().is_empty() {
            return Err(DataFusionError::Execution(
                "required user token not provided".into(),
            ));
        }
        let token_with_user_token = token.clone().with_user_token(user_token.clone());

        let catalog_url = cfg.catalog_url;
        let ims_org = settings.get(IMS_ORG).unwrap().clone();
        let sandbox_name = settings
            .get(SANDBOX_NAME)
            .unwrap_or(&"prod".to_string())
            .clone();
        let catalog_config = CatalogConfig::new(
            catalog_url.clone(),
            client_id.clone(),
            ims_org,
            sandbox_name,
            None,
        );

        let catalog = Arc::new(DynamicAepCatalogProvider::new(
            catalog_config,
            token_with_user_token,
        ));
        ctx.register_catalog("aep", catalog);
        Ok(())
    }

    fn codecs(&self) -> Vec<Box<dyn PhysicalExtensionCodec>> {
        vec![Box::new(AepCodec {})]
    }
}
