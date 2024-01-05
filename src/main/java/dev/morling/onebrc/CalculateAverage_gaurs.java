/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_gaurs {

    private static final String FILE = "./measurements.txt";

    private record Measurement(double min, double max, double sum, long count) {

        Measurement(double initialMeasurement) {
            this(initialMeasurement, initialMeasurement, initialMeasurement, 1);
        }

        public static CalculateAverage_gaurs.Measurement merge(CalculateAverage_gaurs.Measurement m1,
                                                               CalculateAverage_gaurs.Measurement m2) {
            return new CalculateAverage_gaurs.Measurement(
                    Math.min(m1.min, m2.min),
                    Math.max(m1.max, m2.max),
                    m1.sum + m2.sum,
                    m1.count + m2.count
            );
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    public static void main(String[] args) throws IOException {
        Map<String, Measurement> resultMap = new ConcurrentHashMap<>();
        Stream<String> lines = Files.lines(Path.of(FILE), StandardCharsets.UTF_8);

        lines.parallel().forEach(record -> {
            String[] parts = record.split(";");
            String station = parts[0];
            double temperature = Double.parseDouble(parts[1]);
            Measurement measurement = new Measurement(temperature);

            resultMap.compute(station, (key, existingMeasurement) -> existingMeasurement == null ? measurement : Measurement.merge(existingMeasurement, measurement));
        });

        System.out.print("{");

        System.out.print(resultMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Object::toString)
                .collect(Collectors.joining(", ")));

        System.out.println("}");
    }
}
