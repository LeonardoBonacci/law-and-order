package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class PoorAndFlat {

    public String id;
    @Getter public String fkId;
}
