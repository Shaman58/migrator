package ru.vtb.migrator.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ColumnMetaData {
    private String name;
    private String type;
}