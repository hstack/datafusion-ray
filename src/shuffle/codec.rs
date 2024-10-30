// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::protobuf::ray_sql_exec_node::PlanType;
use crate::protobuf::{RayShuffleReaderExecNode, RayShuffleWriterExecNode, RaySqlExecNode};
use crate::shuffle::{RayShuffleReaderExec, RayShuffleWriterExec};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::{self, PhysicalHashRepartition};
use prost::Message;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShuffleCodec {}

impl PhysicalExtensionCodec for ShuffleCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // decode bytes to protobuf struct
        let node = RaySqlExecNode::decode(buf)
            .map_err(|e| DataFusionError::Internal(format!("failed to decode plan: {e:?}")))?;
        let extension_codec = DefaultPhysicalExtensionCodec {};
        if let Some(plan_type) = node.plan_type {
            match plan_type {
                PlanType::RayShuffleReader(reader) => {
                    let schema = reader.schema.as_ref().ok_or_else(|| {
                        DataFusionError::Execution("invalid encoded schema".into())
                    })?;
                    let schema: SchemaRef = Arc::new(schema.try_into()?);
                    let hash_part = parse_protobuf_hash_partitioning(
                        reader.partitioning.as_ref(),
                        registry,
                        &schema,
                        &extension_codec,
                    )?
                    .ok_or_else(|| {
                        DataFusionError::Execution("missing partitioning info".into())
                    })?;
                    Ok(Arc::new(RayShuffleReaderExec::new(
                        reader.stage_id as usize,
                        schema,
                        hash_part,
                    )))
                }
                PlanType::RayShuffleWriter(writer) => {
                    let plan = inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Execution("No inputs for shuffle writer".into())
                        })?
                        .to_owned();
                    let hash_part = parse_protobuf_hash_partitioning(
                        writer.partitioning.as_ref(),
                        registry,
                        plan.schema().as_ref(),
                        &extension_codec,
                    )?
                    .ok_or_else(|| {
                        DataFusionError::Execution("missing partitioning info".into())
                    })?;
                    Ok(Arc::new(RayShuffleWriterExec::new(
                        writer.stage_id as usize,
                        plan,
                        hash_part,
                    )))
                }
            }
        } else {
            Err(DataFusionError::Execution("missing plan_type".into()))
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(reader) = node.as_any().downcast_ref::<RayShuffleReaderExec>() {
            let schema: protobuf::Schema = reader.schema().try_into().unwrap();
            let partitioning =
                encode_partitioning_scheme(reader.properties().output_partitioning())?;
            let reader = RayShuffleReaderExecNode {
                stage_id: reader.stage_id as u32,
                schema: Some(schema),
                partitioning: Some(partitioning),
            };
            Ok(PlanType::RayShuffleReader(reader).encode(buf))
        } else if let Some(writer) = node.as_any().downcast_ref::<RayShuffleWriterExec>() {
            let partitioning =
                encode_partitioning_scheme(writer.properties().output_partitioning())?;
            let writer = RayShuffleWriterExecNode {
                stage_id: writer.stage_id as u32,
                plan: None,
                partitioning: Some(partitioning),
            };
            Ok(PlanType::RayShuffleWriter(writer).encode(buf))
        } else {
            Err(DataFusionError::Execution(format!(
                "Unsupported plan node: {}",
                node.name()
            )))
        }
    }
}

fn encode_partitioning_scheme(partitioning: &Partitioning) -> Result<PhysicalHashRepartition> {
    match partitioning {
        Partitioning::Hash(expr, partition_count) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: expr
                .iter()
                .map(|expr| serialize_physical_expr(expr, &DefaultPhysicalExtensionCodec {}))
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            partition_count: *partition_count as u64,
        }),
        Partitioning::UnknownPartitioning(n) => Ok(protobuf::PhysicalHashRepartition {
            hash_expr: vec![],
            partition_count: *n as u64,
        }),
        other => Err(DataFusionError::Plan(format!(
            "Unsupported shuffle partitioning scheme: {other:?}"
        ))),
    }
}
