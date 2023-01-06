#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script generates the FE calls to populate the builtins.
# To add a builtin, add an entry to impala_functions.py.

import os
from gen_builtins_catalog import FE_PATH

LICENSE = '\
// \n\
//  Licensed under the Apache License, Version 2.0 (the "License");\n\
//  you may not use this file except in compliance with the License.\n\
//  You may obtain a copy of the License at\n\
// \n\
//  http://www.apache.org/licenses/LICENSE-2.0\n\
// \n\
//  Unless required by applicable law or agreed to in writing, software\n\
//  distributed under the License is distributed on an "AS IS" BASIS,\n\
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
//  See the License for the specific language governing permissions and\n\
//  limitations under the License.\n\
\n\
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see the generator at\n\
// common/function-registry/gen_geospatial_builtins_wrappers.py'

PACKAGE_NAME = "org.apache.impala.builtins"

WRAPPERS = [
    [
        "StConvexHullWrapper",
        "org.apache.hadoop.hive.ql.udf.esri.ST_ConvexHull",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable",
        "",
        2,
        8,
        1,
    ],
    [
        "StLineStringWrapper",
        "org.apache.hadoop.hive.ql.udf.esri.ST_LineString",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.hive.serde2.io.DoubleWritable",
        "org.apache.hadoop.hive.ql.exec.UDFArgumentException",
        2,
        14,
        2,
    ],
    [
        "STMultiPointWrapper",
        "org.apache.hadoop.hive.ql.udf.esri.ST_MultiPoint",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.hive.serde2.io.DoubleWritable",
        "org.apache.hadoop.hive.ql.exec.UDFArgumentException",
        2,
        14,
        2,
    ],
    [
        "StPolygonWrapper",
        "org.apache.hadoop.hive.ql.udf.esri.ST_Polygon",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.hive.serde2.io.DoubleWritable",
        "org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException",
        6,
        14,
        2,
    ],
    [
        "STUnionWrapper",
        "org.apache.hadoop.hive.ql.udf.esri.ST_Union",
        "org.apache.hadoop.io.BytesWritable",
        "org.apache.hadoop.io.BytesWritable",
        "",
        2,
        14,
        1,
    ],
]


def generate_parameter(parameter_type, order):
    return "{parameter_type} arg{order}".format(
        parameter_type=parameter_type, order=order
    )


def generate_argument(order):
    return "arg%d" % order


def generate_argument_list(order):
    arguments = list()
    for i in range(order):
        arguments.append(generate_argument(i))
    return ",".join(arguments)


def generate_parameter_list(parameter_type, order):
    parameters = list()
    for i in range(order):
        parameters.append(generate_parameter(parameter_type, i))
    return ",".join(parameters)


def generate_method(return_type, parameter_type, exception_type, order):
    exception_clause = ""
    if exception_type:
        exception_clause = "throws {exception_type}".format(
            exception_type=exception_type
        )
    return '''public {return_type} evaluate({parameter_list}) {exception_clause}
        {{ return super.evaluate({argument_list});}}'''.format(
        return_type=return_type,
        parameter_list=generate_parameter_list(parameter_type, order),
        exception_clause=exception_clause,
        argument_list=generate_argument_list(order),
    )


def generate_methods(
    return_type,
    parameter_type,
    exception_type,
    min_number_of_params,
    max_number_of_params,
    increment,
):
    methods = list()

    for i in range(min_number_of_params, max_number_of_params + 1, increment):
        methods.append(generate_method(return_type, parameter_type, exception_type, i))

    return " ".join(methods)


def generate_wrapper_class(config):
    return "public class {class_name} extends {base_class} {{ {methods} }}".format(
        class_name=config[0],
        base_class=config[1],
        methods=generate_methods(
            config[2], config[3], config[4], config[5], config[6], config[7]
        ),
    )


def generate_file(config):
    return "{license}\npackage {package};\n {wrapper_class}".format(
        license=LICENSE,
        package=PACKAGE_NAME,
        wrapper_class=generate_wrapper_class(config),
    )


if not os.path.exists(FE_PATH):
    os.makedirs(FE_PATH)

for wrapper_config in WRAPPERS:
    filename = "%s.java" % wrapper_config[0]
    path = os.path.join(FE_PATH, filename)
    wrapper_class_file = open(path, "w")
    wrapper_class_file.write(generate_file(wrapper_config))
    wrapper_class_file.close()
