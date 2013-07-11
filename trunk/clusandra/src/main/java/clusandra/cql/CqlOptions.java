package clusandra.cql;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CqlOptions {
	
	private static CLIOptions options = null; // Info about command line options

    // Name of the command line tool (used for error messages)
    private static final String TOOL_NAME = "clusandra-cql";

    // Command line options
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final String DEBUG_OPTION = "debug";
    private static final String USERNAME_OPTION = "username";
    private static final String PASSWORD_OPTION = "password";
    private static final String KEYSPACE_OPTION = "keyspace";
    private static final String HELP_OPTION = "help";
    private static final String FILE_OPTION = "file";

    // Default values for optional command line arguments
    private static final int    DEFAULT_THRIFT_PORT = 9160;

    // Register the command line options and their properties (such as
    // whether they take an extra argument, etc.
    static
    {
        options = new CLIOptions();

        options.addOption("h",  HOST_OPTION,     "HOSTNAME", "cassandra server's host name");
        options.addOption("p",  PORT_OPTION,     "PORT",     "cassandra server's thrift port");
        options.addOption("u",  USERNAME_OPTION, "USERNAME", "user name for cassandra authentication");
        options.addOption("pw", PASSWORD_OPTION, "PASSWORD", "password for cassandra authentication");
        options.addOption("k",  KEYSPACE_OPTION, "KEYSPACE", "cassandra keyspace user is authenticated against");
        options.addOption("f",  FILE_OPTION,     "FILENAME", "load statements from the specific file");
        

        // options without argument
        options.addOption(null, DEBUG_OPTION,   "display stack traces");
        options.addOption("?",  HELP_OPTION,    "usage help");
    }

    private static void printUsage()
    {
        new HelpFormatter().printHelp(TOOL_NAME, options);
    }

    public void processArgs(CqlSessionState css, String[] args)
    {
        CommandLineParser parser = new GnuParser();

        try
        {
            CommandLine cmd = parser.parse(options, args, false);

            if (cmd.hasOption(HOST_OPTION))
            {
                css.hostName = cmd.getOptionValue(HOST_OPTION);
            }
            else
            {
                // host name not specified in command line.
                // In this case, we don't implicitly connect at CQL startup. In this case,
                // the user must use the "connect" CQL statement to connect.
                css.hostName = null;
            }


            // Look to see if frame has been specified
            if (cmd.hasOption(DEBUG_OPTION))
            {
                css.debug = true;
            }

            // Look for optional args.
            if (cmd.hasOption(PORT_OPTION))
            {
                css.thriftPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
            }
            else
            {
                css.thriftPort = DEFAULT_THRIFT_PORT;
            }

            // Look for authentication credentials (username and password)
            if (cmd.hasOption(USERNAME_OPTION))
            {
            	css.username = cmd.getOptionValue(USERNAME_OPTION);
            }
            else
            {
                css.username = "default";
            }

            if (cmd.hasOption(PASSWORD_OPTION))
            {
            	css.password = cmd.getOptionValue(PASSWORD_OPTION);
            }
            else
            {
                css.password = "";
            }

            // Look for keyspace
            if (cmd.hasOption(KEYSPACE_OPTION))
            {
            	css.keyspace = cmd.getOptionValue(KEYSPACE_OPTION);
            }
      

            if (cmd.hasOption(FILE_OPTION))
            {
                css.filename = cmd.getOptionValue(FILE_OPTION);
            }
  

            if (cmd.hasOption(HELP_OPTION))
            {
                printUsage();
                System.exit(1);
            }


            // Abort if there are any unrecognized arguments left
            if (cmd.getArgs().length > 0)
            {
                System.err.printf("Unknown argument: %s\n", cmd.getArgs()[0]);
                System.err.println();
                printUsage();
                System.exit(1);
            }
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            System.err.println();
            printUsage();
            System.exit(1);
        }
    }

    private static class CLIOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }


}
