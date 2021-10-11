package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("districtsTrees", DistrictsTrees.class, "A map/reduce program that displays the list of distinct containing trees in the input file.");

            programDriver.addClass("treeSpecies", ExistingSpecies.class,
                    "A map/reduce program that that displays the list of different species trees in the input file.");

            programDriver.addClass("treeKindsCount", TreeKindsCount.class,
                    "A map/reduce program that calculates the number of trees of each kinds in the input file.");

            programDriver.addClass("maxHeightKinds", MaxHeightKinds.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind in the input file");
            programDriver.addClass("sortedHeight", SortedHeight.class,
                    "A map/reduce program that sort the trees height from smallest to largest in the input file");
            programDriver.addClass("districtOldesTree", DistrictOldesTree.class,
                    "A map/reduce program that displays the district where the oldest tree is in the input file.");
            programDriver.addClass("districtMaxTrees", DistrictMaxTrees.class,
                    "A map/reduce program that displays the district that contains the most trees in the input file.");




            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}