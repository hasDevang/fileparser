package com.tune.shoveler;

import java.io.File;

import com.tune.shoveler.Shoveler;

import uk.co.flamingpenguin.jewel.cli.ArgumentValidationException;
import uk.co.flamingpenguin.jewel.cli.CliFactory;
import uk.co.flamingpenguin.jewel.cli.Option;

public class ShovelerCLI {
	
	Shoveler shoveler;
	private interface Arguments {

        @Option( shortName="p", longName = "producer", description = "Number of producer", defaultValue="1" )
        int getProducer();
        
        @Option( shortName="c", longName = "consumer", description = "Number of comsumer" , defaultValue = "1" )
        int getConsumer();
        
        @Option( shortName="a", longName = "awsproperties", description = "aws property file" )
        File getAwsProperties();
        
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
        Shoveler shoveler = new Shoveler(args.getConsumer(), args.getProducer(), args.getAwsProperties());
        shoveler.run();
    }
}
