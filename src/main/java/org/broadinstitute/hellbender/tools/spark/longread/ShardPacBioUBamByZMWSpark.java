package org.broadinstitute.hellbender.tools.spark.longread;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Advanced;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LongReadAnalysisProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVFileUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

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


    private static final String ZMW_ATRRIBUTE_KEY = "zm";


    @Argument(doc = "the output prefix", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    protected String output;

    @Advanced
    @Argument(doc = "count of ZMWs in one shard output bam", fullName = "shard-size", optional = true)
    protected Integer shardSize = 20_000;

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        return Arrays.asList(new ReadFilterLibrary.AllowAllReadsReadFilter());
    }



    @Override
    protected void runTool( final JavaSparkContext ctx ) {

        final SAMFileHeader headerForReads = getHeaderForReads();

        JavaPairRDD<String, Iterable<SAMRecord>> readsGroupedByZMW =
                getUnfilteredReads().map(read -> read.convertToSAMRecord(headerForReads))
                        .groupBy(read -> read.getStringAttribute(ZMW_ATRRIBUTE_KEY));

        JavaPairRDD<Integer, Iterable<Tuple2<String, Iterable<SAMRecord>>>> clusteredByShard =
                readsGroupedByZMW
                        .mapToPair(pair -> {
                            final Integer zmw = new Integer(pair._1);
                            int idx = zmw / shardSize;
                            return new Tuple2<>(idx, pair);
                        })
                        .groupByKey();

        clusteredByShard.foreach(cluster -> {

            final String name = output + cluster._1;
            final Iterator<SAMRecord> readsInThisCluster = StreamSupport.stream(cluster._2.spliterator(), false)
                    .flatMap(i -> StreamSupport.stream(i._2.spliterator(), false))
                    .iterator();
            SVFileUtils.writeSAMFile(name, readsInThisCluster, headerForReads, false);

        });
    }
}
