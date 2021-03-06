package fr.compo;

import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class MainAppTest {

    @Test
    public void shouldReadFile() throws IOException {
        MainApp.main(new String[0]);
        assertThat(
                Files.list(
                        Paths.get(ConfigFactory.load().getString("3il.path.output").replace("file://", ""))
                ).count()
        ).isGreaterThan(0L);
    }
}
