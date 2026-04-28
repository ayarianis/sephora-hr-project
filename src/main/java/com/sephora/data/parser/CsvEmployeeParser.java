package com.sephora.data.parser;

import com.sephora.data.model.Employee;
import com.sephora.data.service.EmployeeService;

import java.util.List;

public class CsvEmployeeParser implements SephoraFileParser<Employee> {

    @Override
    public List<Employee> parse(String filePath) throws Exception {
        EmployeeService service = new EmployeeService();
        return service.loadEmployeesFromCsv(filePath);
    }
}