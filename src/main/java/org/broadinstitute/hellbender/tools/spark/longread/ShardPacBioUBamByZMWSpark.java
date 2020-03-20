package org.broadinstitute.hellbender.tools.spark.longread;

import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.programgroups.LongReadAnalysisProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;

/**
 * Subsets reads by name (basically a parallel version of "grep -f", or "grep -vf")
 *
 * <p>Reads a file of read (i.e., template) names, and searches a SAM/BAM/CRAM to find names that match.
 * The matching reads are copied to an output file.</p>
 * <p>Unlike FilterSamReads (Picard), this tool can take input reads in any order
 * (e.g., unsorted or coordinate-sorted).</p>
 *
 * <h3>Inputs</h3>
 * <ul>
 *     <li>An input file of read names.</li>
 *     <li>An input file of reads.</li>
 * </ul>
 *
 * <h3>Output</h3>
 * <ul>
 *     <li>A file containing matching reads.</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 * <pre>
 *   gatk ExtractOriginalAlignmentRecordsByNameSpark \
 *     -I input_reads.bam \
 *     --read-name-file read_names.txt \
 *     -O output_reads.bam
 * </pre>
 * <p>This tool can be run without explicitly specifying Spark options. That is to say, the given example command
 * without Spark options will run locally. See
 * <a href ="https://software.broadinstitute.org/gatk/documentation/article?id=10060">Tutorial#10060</a>
 * for an example of how to set up and run a Spark tool on a cloud Spark cluster.</p>
 */
@DocumentedFeature
@BetaFeature
@CommandLineProgramProperties(
        oneLineSummary = "Subsets reads by name",
        summary =
                "Reads a file of read (i.e., template) names, and searches a SAM/BAM/CRAM to find names that match." +
                        " The matching reads are copied to an output file." +
                        " Unlike FilterSamReads (Picard), this tool can take input reads in any order" +
                        " (e.g., unsorted or coordinate-sorted).",
        programGroup = LongReadAnalysisProgramGroup.class)
public class ShardPacBioUBamByZMWSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    protected void runTool( final JavaSparkContext ctx ) {

    }
}
