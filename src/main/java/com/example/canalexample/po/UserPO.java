package com.example.canalexample.po;

import lombok.Data;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;

@Data
@Document(indexName = "user_idx", type = "doc", shards = 1, replicas = 2, refreshInterval = "-1")
public class UserPO implements Serializable {

    private static final long serialVersionUID = 7783661500244414255L;

    @Id
    private int id;

    @Field(type = FieldType.Text)
    private String name;

    @Field(type = FieldType.Integer)
    private int roleId;

    @Field(type = FieldType.Keyword)
    private String roleName;

}
