// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;

int assignmentEvaluations = 0;
int returnEvaluations = 0;
int argumentEvaluations = 0;

function assignmentValue(int value) returns int {
    assignmentEvaluations += 1;
    return value * 10;
}

function returnValue(int value) returns int {
    returnEvaluations += 1;
    return value * 10;
}

function argumentValue(int value) returns int {
    argumentEvaluations += 1;
    return value * 10;
}

function contextualStream() returns stream<int> {
    return from int value in [3, 4]
        select returnValue(value);
}

function consumeStream(stream<int> values) returns record {|int value;|}? {
    return values.next();
}

function checkedValue(int value) returns int|error {
    if value == 0 {
        return error("checked completion");
    }
    return value;
}

public class FailingIterator {
    int nextValue = 1;

    public isolated function next() returns record {|int value;|}|error? {
        if self.nextValue > 1 {
            return error("source completion");
        }
        int value = self.nextValue;
        self.nextValue += 1;
        return {value: value};
    }
}

function contextualSourceStream(stream<int, error?> sourceStream)
        returns stream<int, error?> {
    return from int value in sourceStream
        select value * 10;
}

public function main() {
    stream<int> assigned = from int value in [1, 2]
        select assignmentValue(value);
    io:println(assignmentEvaluations); // @output 0
    io:println(assigned.next()); // @output {"value":10}
    io:println(assignmentEvaluations); // @output 1
    io:println(assigned.next()); // @output {"value":20}

    stream<int> returned = contextualStream();
    io:println(returnEvaluations); // @output 0
    io:println(returned.next()); // @output {"value":30}
    io:println(returnEvaluations); // @output 1

    io:println(argumentEvaluations); // @output 0
    io:println(consumeStream(from int value in [5]
        select argumentValue(value))); // @output {"value":50}
    io:println(argumentEvaluations); // @output 1

    stream<int, error?> checked = from int value in [0, 1]
        select check checkedValue(value);
    io:println(checked.next()); // @output error("checked completion")
    io:println(checked.next()); // @output {"value":1}

    var sourceStream = new stream<int, error?>(new FailingIterator());
    stream<int, error?> sourced = contextualSourceStream(sourceStream);
    io:println(sourced.next()); // @output {"value":10}
    io:println(sourced.next()); // @output error("source completion")
}
