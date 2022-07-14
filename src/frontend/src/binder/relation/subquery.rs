// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Query, TableAlias};

use crate::binder::{Binder, BoundQuery, UNNAMED_SUBQUERY};

#[derive(Debug, Clone)]
pub struct BoundSubquery {
    pub query: BoundQuery,
}

impl Binder {
    /// Binds a subquery using [`bind_query`](Self::bind_query), which will use a new empty
    /// [`BindContext`](crate::binder::BindContext) for it.
    ///
    /// After finishing binding, we update the current context with the output of the subquery.
    pub(super) fn bind_subquery_relation(
        &mut self,
        query: Query,
        alias: Option<TableAlias>,
    ) -> Result<BoundSubquery> {
        let query = self.bind_query(query)?;
        let sub_query_id = self.next_subquery_id();

        self.bind_table_to_context(
            query
                .body
                .schema()
                .fields
                .iter()
                .map(|f| (false, f.clone())),
            format!("{}_{}", UNNAMED_SUBQUERY, sub_query_id),
            alias,
        )?;
        Ok(BoundSubquery { query })
    }
}
