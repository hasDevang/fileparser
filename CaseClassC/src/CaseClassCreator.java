import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author fuat
 * 
 */
public class CaseClassCreator {

	static ArrayList<String> methodList = new ArrayList<String>();

	/**
	 * Takes a file and generates case class file from the given file.
	 * @param args the args.
	 * @throws IOException IO exception.
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {

		if (args.length == 0 || args.length > 2) {
			System.out
					.println("To run this jar: java -jar <jarname> <input file path>  <output file path>");
			System.exit(0);
		}

		ArrayList<String[]> list = new ArrayList<String[]>();

		File infile = new File(args[0]);// "/home/fuat/logs.txt");
		File outfile = new File(args[1]);// "/home/fuat/caseClass");

		Scanner in = new Scanner(infile);
		FileWriter fw = new FileWriter(outfile);
		PrintWriter pw = new PrintWriter(fw);

		while (in.hasNext()) {

			String sline = in.nextLine();

			if (!sline.contains(":")) {

				continue;

			}
			String[] line = sline.replaceAll(";", "").trim().split(" : ");

			if (line[1].equals("string")) {
				line[1] = "String";
			}

			else if (line[1].equals("boolean") || line[1].equals("bool")) {
				line[1] = "Boolean";
			} else if (line[1].equals("int")) {
				line[1] = "Int";
			} else if (line[1].equals("float")) {
				line[1] = "Float";
			} else {
				line[1] = Character.toUpperCase(line[1].charAt(0))
						+ line[1].substring(1);
			}
			// System.out.println(line[1]);
			list.add(line);

			//// pw.println("case class "+line[0]+"( var "+line[0]+": "+line[1]+")");

		}

		String[] fields = list.get(0);
		StringBuilder str = new StringBuilder("case class RawLog \n( var "
				+ fields[0] + ": " + fields[1]);

		convertToMethod(fields);
		StringBuilder str1 = new StringBuilder(
				"def this(rl : rawLog) = this ( rl." + methodList.get(0));

		for (int i = 1; i < list.size(); i++) {

			fields = list.get(i);
			convertToMethod(fields);

			str.append(", \n");
			str.append("var ");
			str.append(fields[0]);
			str.append(": ");
			str.append(fields[1]);

			str1.append(", \n");
			str1.append("rl." + methodList.get(i));

			/*
			 * if(i%10==0 ){ str.append("\n"); str1.append("\n"); }
			 */

		}

		str.append(") {  \n\n");
		str1.append(") ");
		str.append(str1 + "\n}");

		pw.write(str.toString());

		pw.close();

	}

	/**
	 * 
	 * @param fields a string array each of which includes filed and types.
	 */

	private static void convertToMethod(String[] fields) {

		StringBuilder str2 = new StringBuilder();

		String[] methodName = fields[0].split("_");
		// str2.append("(");
		// method creator.
		for (int k = 0; k < methodName.length; k++) {

			if (k > 0) {
				methodName[k] = Character.toUpperCase(methodName[k].charAt(0))
						+ methodName[k].substring(1);
				str2.append(methodName[k]);

			} else {
				str2.append(methodName[k]);
			}
		}

		str2.append("()");
		methodList.add(str2.toString());

	}

}
