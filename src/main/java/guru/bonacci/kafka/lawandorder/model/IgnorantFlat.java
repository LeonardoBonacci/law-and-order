package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class IgnorantFlat {

    public String id;
    @Getter public String fkId;
}
