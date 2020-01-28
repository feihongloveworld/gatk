package org.broadinstitute.hellbender.tools.spark.longread;

import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LongReadAnalysisProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Given a long read assembly bam, split the input bam by contig name (one contig one split),
 * and save the split bams in a specified directory.
 *
 * The reason for this tool is that we constantly run into out-of-memory errors on the whole bam.
 */
@DocumentedFeature
@BetaFeature
@CommandLineProgramProperties(
        oneLineSummary = "Split long read assembly bam by contig name",
        summary = "Split long read assembly bam by contig name",
        programGroup = LongReadAnalysisProgramGroup.class)
public class SplitLongReadAssemblyBAMByContigSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() {
        return true;
    }

    /**
     * This is overriden because the alignment might be not "WellFormed".
     */
    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        return Arrays.asList(ReadFilterLibrary.ALLOW_ALL_READS);
    }


    @Argument(doc = "uri for the output dir",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private String outdir;

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final SAMFileHeader headerForReads = getHeaderForReads();
        final Path referencePath = referenceArguments.getReferencePath();

        getReads()
                .groupBy(GATKRead::getName)
                .foreach(pair -> writeAlignmentsForOneContig(outdir, referencePath, headerForReads, pair));
    }

    // write alignments for each contig
    private static void writeAlignmentsForOneContig(final String outdir,
                                                    final Path referencePath,
                                                    final SAMFileHeader headerForReads,
                                                    final Tuple2<String, Iterable<GATKRead>> readGroupedByName) {
        final String readName = readGroupedByName._1();
        final Iterable<GATKRead> gatkReads = readGroupedByName._2();
        final String output = outdir + "." + readName + ".bam";
        try (final SAMFileGATKReadWriter outputWriter = new SAMFileGATKReadWriter(
                ReadUtils.createCommonSAMWriter(
                        IOUtils.getPath(output),
                        referencePath,
                        headerForReads,
                        false,
                        true,
                        false
                )
        )){
            gatkReads.forEach(outputWriter::addRead);
        }
    }
}
