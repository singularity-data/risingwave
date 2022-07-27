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

use std::ffi::OsStr;

use libtest_mimic::{run_tests, Arguments, Outcome, Test};
use risingwave_frontend_test_runner::run_test_file;
use walkdir::WalkDir;

#[cfg(not(madsim))]
fn main() {
    let run_tests_args = &Arguments::from_args();
    let mut tests = vec![];

    for entry in WalkDir::new("tests/testdata") {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        if (path.extension() == Some(OsStr::new("yml"))
            || path.extension() == Some(OsStr::new("yaml")))
            && !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".apply.yaml")
        {
            let file_name = path.file_name().unwrap().to_string_lossy();
            let test_case_name = file_name.split('.').next().unwrap();
            tests.push(Test {
                name: format!("{test_case_name}_test"),
                kind: "".into(),
                is_ignored: false,
                is_bench: false,
                data: (test_case_name.to_string(), file_name.to_string()),
            });
        }
    }

    if tests.is_empty() {
        panic!("no test case found in planner test!");
    }

    run_tests(run_tests_args, tests, |test| {
        let (test_case_name, file_name): (String, String) = test.clone().data;
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("testdata")
            .join(file_name);

        let file_content = std::fs::read_to_string(path).unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_test_file(&test_case_name, &file_content));
        Outcome::Passed
    })
    .exit();
}

#[cfg(madsim)]
fn main() {
    // println!("planner test is not supported yet in simulation");
    run_tests(&Arguments::from_args(), vec![], |_: &Test<()>| {
        Outcome::Passed
    })
    .exit();
}
