package com.tune.shoveler;

import java.io.File;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;
import com.tune.shoveler.Shoveler;

/**
 * This class provides command line interface to run Shoveler
 * Shoveler consumes messages from SQS to feed into Kafka
 *
 */
public class ShovelerCLI {
	
	Shoveler shoveler;
	private interface Arguments {

        @Option( shortName="p", longName = "producer", description = "Number of producer", defaultValue="1" )
        int getProducer();
        
        @Option( shortName="c", longName = "consumer", description = "Number of comsumer" , defaultValue = "1" )
        int getConsumer();
        
        @Option( shortName="a", longName = "awsproperties", description = "aws property file" )
        File getAwsProperties();
        
        @Option( shortName="q", longName = "queue", description = "Blocking queue name", defaultValue = "prod_measured_raw" )
        String getQueue();
        
        @Option( shortName="s", longName = "size", description = "pool size", defaultValue = "30" )
        int getSize();
        
        @Option(helpRequest = true, description = "Print help message")
        boolean getHelp();
        
	}
	
	private static Arguments parseArgs(String[] args) {
        try {
                return CliFactory.parseArguments(Arguments.class, args);
        } catch (ArgumentValidationException e) {
                System.err.println(e.getMessage());
                System.exit(1);
        }
        return null;
}

	
   
    public static void main(String[] commandLineArgs) {
    	Arguments args = parseArgs(commandLineArgs);
        Shoveler shoveler = new Shoveler(args.getConsumer(), args.getProducer(), args.getAwsProperties(), args.getQueue(), args.getSize());
        shoveler.run();
    }
}
