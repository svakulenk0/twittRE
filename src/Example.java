import java.io.IOException;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Clause;
import de.mpii.clausie.Options;
import de.mpii.clausie.Proposition;

public class Example {
    public static void main(String[] args) throws IOException {
        ClausIE clausIE = new ClausIE();
        clausIE.initParser();
        Options my_options = clausIE.getOptions(); //.print(System.out, "# ");
//        my_options.nary = true;
        my_options.lemmatize = true;
        my_options.conservativeSVOA = false;
//        my_options.processCcNonVerbs = false; // splits into 2
        my_options.processCcAllVerbs = false;
//        my_options.processAppositions = true; // , is
        my_options.processPossessives = false; // your has
//        my_options.processPartmods = false;
        my_options.print(System.out, "# ");

        // input sentence
        // String sentence =
        // "Bell, a telecommunication company, which is based in Los Angeles, makes and distributes electronic, computer and building products.";
        // String sentence = "There is a ghost in the room";
        // sentence = "Bell sometimes makes products";
        // sentence = "By using its experise, Bell made great products in 1922 in Saarland.";
        // sentence = "Albert Einstein remained in Princeton.";
        // sentence = "Albert Einstein is smart.";
        String sentence = "He will break his leg"; //AkhilDhanania: Chelsea are the 2012 FACupChampions!";
        System.out.println("Input sentence   : " + sentence);

        // parse tree
        System.out.print("Parse time       : ");
        long start = System.currentTimeMillis();
        clausIE.parse(sentence);
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1000. + "s");
        System.out.print("Dependency parse : ");
        System.out.println(clausIE.getDepTree().pennString()
                .replaceAll("\n", "\n                   ").trim());
        System.out.print("Semantic graph   : ");
        System.out.println(clausIE.getSemanticGraph().toFormattedString()
                .replaceAll("\n", "\n                   ").trim());

        // clause detection
        System.out.print("ClausIE time     : ");
        start = System.currentTimeMillis();
        clausIE.detectClauses();
        clausIE.generatePropositions();
        end = System.currentTimeMillis();
        System.out.println((end - start) / 1000. + "s");
        System.out.print("Clauses          : ");
        String sep = "";
        for (Clause clause : clausIE.getClauses()) {
            System.out.println(sep + clause.toString(clausIE.getOptions()));
            sep = "                   ";
        }

        // generate propositions
        System.out.print("Propositions     : ");
        sep = "";
        for (Proposition prop : clausIE.getPropositions()) {
            System.out.println(sep + prop.toString());
            sep = "                   ";
        }
    }
}
