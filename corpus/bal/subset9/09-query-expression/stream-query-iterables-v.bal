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

public class RangeIterator {
    int nextValue = 1;

    public isolated function next() returns record {|int value;|}? {
        if self.nextValue > 3 {
            return ();
        }
        int value = self.nextValue;
        self.nextValue += 1;
        return {value: value};
    }
}

class RangeIterable {
    *object:Iterable;
    RangeIterator iteratorValue;

    function init(RangeIterator iteratorValue) {
        self.iteratorValue = iteratorValue;
    }

    public function iterator() returns RangeIterator {
        return self.iteratorValue;
    }
}

public class FailingStreamIterator {
    int nextValue = 1;
    int closeCount = 0;

    public isolated function next() returns record {|int value;|}|error? {
        if self.nextValue > 2 {
            return error("source completion");
        }
        int value = self.nextValue;
        self.nextValue += 1;
        return {value: value};
    }

    public isolated function close() {
        self.closeCount += 1;
    }
}

public class TrackedStreamIterator {
    int nextValue = 1;
    int closeCount = 0;

    public isolated function next() returns record {|int value;|}? {
        if self.nextValue > 3 {
            return ();
        }
        int value = self.nextValue;
        self.nextValue += 1;
        return {value: value};
    }

    public isolated function close() {
        self.closeCount += 1;
    }
}

public function main() {
    stream<int> listValues = stream from int value in [1, 2]
        select value * 10;
    io:println(listValues.next()); // @output {"value":10}
    io:println(listValues.next()); // @output {"value":20}

    map<int> numbers = {one: 1, two: 2};
    stream<int> mapValues = stream from int value in numbers
        select value;
    io:println(mapValues.next()); // @output {"value":1}
    io:println(mapValues.next()); // @output {"value":2}

    stream<string> characters = stream from string character in "A\u{1F600}"
        select character;
    io:println(characters.next()); // @output {"value":"A"}
    io:println(characters.next()); // @output {"value":"😀"}

    stream<xml> xmlItems = stream from var item in xml `<a/>text`
        select item;
    io:println(xmlItems.next()); // @output {"value":<a/>}
    io:println(xmlItems.next()); // @output {"value":text}

    RangeIterator objectIterator = new;
    stream<int> objectValues = stream from int value in new RangeIterable(objectIterator)
        limit 1
        select value;
    io:println(objectValues.next()); // @output {"value":1}
    io:println(objectValues.next()); // @output
    io:println(objectIterator.nextValue); // @output 2
    _ = objectValues.close();

    TrackedStreamIterator trackedIterator = new;
    var trackedStream = new stream<int>(trackedIterator);
    stream<int> limitedStreamValues = stream from int value in trackedStream
        limit 1
        select value;
    io:println(limitedStreamValues.next()); // @output {"value":1}
    io:println(limitedStreamValues.next()); // @output
    io:println(trackedIterator.nextValue); // @output 2
    io:println(trackedIterator.closeCount); // @output 1
    _ = limitedStreamValues.close();
    io:println(trackedIterator.closeCount); // @output 1

    FailingStreamIterator sourceIterator = new;
    var sourceStream = new stream<int, error?>(sourceIterator);
    stream<int, error?> sourceValues = stream from int value in sourceStream
        select value * 10;
    io:println(sourceValues.next()); // @output {"value":10}
    io:println(sourceValues.next()); // @output {"value":20}
    io:println(sourceValues.next()); // @output error("source completion")
    io:println(sourceIterator.closeCount); // @output 1
    io:println(sourceValues.next()); // @output error("source completion")
    io:println(sourceIterator.closeCount); // @output 1

    FailingStreamIterator joinIterator = new;
    var joinStream = new stream<int, error?>(joinIterator);
    stream<int, error?> joinedValues = stream from int left in [1, 2]
        join int right in joinStream
        on left equals right
        select left + right;
    io:println(joinedValues.next()); // @output error("source completion")
    io:println(joinIterator.closeCount); // @output 1
}
