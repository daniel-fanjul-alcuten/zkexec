package fanjul.daniel.zkexec.cli;

import java.util.List;

import uk.co.flamingpenguin.jewel.cli.Option;
import uk.co.flamingpenguin.jewel.cli.Unparsed;

public interface ZKExecOptions {

    @Option(shortName = "z", defaultValue = "localhost:2181", description = "zookeeper connection string")
    String getConnectionString();

    @Option(shortName = "t", defaultValue = "5000", description = "zookeeper session timeout in milliseconds")
    int getSessionTimeout();

    @Option(shortName = "n", description = "name of the znode whose data contain the max number of processes to run and uses its children to run")
    String getZNodeName();

    @Option(shortName = "q", description = "suppress error messages")
    boolean getQuiet();

    @Option(shortName = "v", description = "show info and warning messages")
    boolean getVerbose();

    @Option(shortName = "h", helpRequest = true, description = "show available options")
    boolean getHelp();

    @Unparsed(name = "command line")
    List<String> getCommandLine();
}
