package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Rich {

    public PoorAndFlat paf;
    public NestedNode nn;
}
