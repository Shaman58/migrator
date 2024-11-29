package ru.vtb.migrator;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MigrationRunner implements CommandLineRunner {

    private final MigrationService migrationService;

    @Override
    public void run(String... args) throws Exception {
        migrationService.migrate("tables.txt");
    }
}