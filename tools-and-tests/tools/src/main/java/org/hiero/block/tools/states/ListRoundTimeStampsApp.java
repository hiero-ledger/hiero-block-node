package org.hiero.block.tools.states;

import org.hiero.block.tools.states.model.SignedState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Application to scan a directory for saved states and find the state nearest but before the first
 * transaction of block zero.,
 */
public class ListRoundTimeStampsApp {
    public static void main(String[] args) throws IOException {
        final long firstTransactionSeconds = 1568411616;
        final long firstTransactionNanos = 448357000;
        final Instant firstTransactionTime = Instant.ofEpochSecond(firstTransactionSeconds, firstTransactionNanos);

        final Path userHome = Path.of(System.getProperty("user.home"));
        final Path stateFilesDir = userHome.resolve("code/Sample Blocks/mainnet-state-OA");
        List<SavedStateInfo> results = new ArrayList<>();
        Files.walk(stateFilesDir)
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().equals("SignedState.swh"))
                .forEach(signedStateFile -> {
                    try {
                        SignedState signedState = Main.loadSignedState(signedStateFile);
                        // Collecting the round and consensus timestamp
                        SavedStateInfo savedStateInfo = new SavedStateInfo(
                                signedState.round(),
                                signedState.consensusTimestamp().getEpochSecond(),
                                signedState.consensusTimestamp().getNano(),
                                signedState.consensusTimestamp(),
                                Duration.between(firstTransactionTime,signedState.consensusTimestamp()),
                                signedStateFile.getParent());
                        results.add(savedStateInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
        // sort results by round
        results.sort((a, b) -> Long.compare(a.round, b.round));
        // print the results
        results.forEach(System.out::println);
        // find the nearest round before the first transaction
        SavedStateInfo nearestRound = results.stream()
                .filter(info -> info.consensusTimestampSeconds < firstTransactionSeconds ||
                        (info.consensusTimestampSeconds == firstTransactionSeconds &&
                                info.consensusTimestampNanos <= firstTransactionNanos))
                .max((a, b) -> Long.compare(a.round, b.round))
                .orElse(null);
        System.out.println("nearestRound = " + nearestRound);
    }
    record SavedStateInfo(long round, long consensusTimestampSeconds, int consensusTimestampNanos,
                          Instant consensusTimestamp, Duration deltaFromFirstTransaction, Path path) {
    }
}
/*
round = 33312259, consensusTimestamp = 1568331900.310557000
round = 33314495, consensusTimestamp = 1568332800.90006000
round = 33316723, consensusTimestamp = 1568333700.281509000
round = 33318974, consensusTimestamp = 1568334600.35766000
round = 33321229, consensusTimestamp = 1568335500.105628000
round = 33323469, consensusTimestamp = 1568336400.330395000
round = 33325706, consensusTimestamp = 1568337300.277771000
round = 33327945, consensusTimestamp = 1568338200.31910000
round = 33330186, consensusTimestamp = 1568339100.223114000
round = 33332423, consensusTimestamp = 1568340000.2123000
round = 33334664, consensusTimestamp = 1568340900.403495000
round = 33336902, consensusTimestamp = 1568341800.103334000
round = 33339158, consensusTimestamp = 1568342700.25493000
round = 33341400, consensusTimestamp = 1568343600.167432000
round = 33343643, consensusTimestamp = 1568344500.15044000
round = 33345884, consensusTimestamp = 1568345400.77957001
round = 33348120, consensusTimestamp = 1568346300.389057000
round = 33350358, consensusTimestamp = 1568347200.197224000
round = 33352591, consensusTimestamp = 1568348100.137468000
round = 33354833, consensusTimestamp = 1568349000.132270000
round = 33357074, consensusTimestamp = 1568349900.319368000
round = 33359313, consensusTimestamp = 1568350800.135871000
round = 33361549, consensusTimestamp = 1568351700.245975000
round = 33363783, consensusTimestamp = 1568352600.385098000
round = 33366022, consensusTimestamp = 1568353500.177980000
round = 33368264, consensusTimestamp = 1568354400.77408000
round = 33370471, consensusTimestamp = 1568355300.141452000
round = 33372708, consensusTimestamp = 1568356200.312941000
round = 33374935, consensusTimestamp = 1568357100.340908000
round = 33377176, consensusTimestamp = 1568358000.341029001
round = 33379433, consensusTimestamp = 1568358900.183164000
round = 33381693, consensusTimestamp = 1568359800.17574000
round = 33383950, consensusTimestamp = 1568360700.220783000
round = 33386202, consensusTimestamp = 1568361600.252694000
round = 33388459, consensusTimestamp = 1568362500.25580000
round = 33390707, consensusTimestamp = 1568363400.161949000
round = 33392968, consensusTimestamp = 1568364300.95088000
round = 33395232, consensusTimestamp = 1568365200.63858000
round = 33397492, consensusTimestamp = 1568366100.302428000
round = 33399749, consensusTimestamp = 1568367000.43134001
round = 33402012, consensusTimestamp = 1568367900.240099000
round = 33404269, consensusTimestamp = 1568368800.204186000
round = 33406528, consensusTimestamp = 1568369700.35984000
round = 33408791, consensusTimestamp = 1568370600.187062000
round = 33411053, consensusTimestamp = 1568371500.98571001
round = 33413301, consensusTimestamp = 1568372400.223683001
round = 33415558, consensusTimestamp = 1568373300.356940000
round = 33417822, consensusTimestamp = 1568374200.226386000
round = 33420081, consensusTimestamp = 1568375100.223712001
round = 33422344, consensusTimestamp = 1568376000.149308000
round = 33424605, consensusTimestamp = 1568376900.234671000
round = 33426859, consensusTimestamp = 1568377800.216846000
round = 33429112, consensusTimestamp = 1568378700.390897001
round = 33431359, consensusTimestamp = 1568379600.90952000
round = 33433607, consensusTimestamp = 1568380500.74080000
round = 33435860, consensusTimestamp = 1568381400.304875000
round = 33438120, consensusTimestamp = 1568382300.19727000
round = 33440372, consensusTimestamp = 1568383200.162390000
round = 33442625, consensusTimestamp = 1568384100.189904000
round = 33444862, consensusTimestamp = 1568385000.68646000
round = 33447113, consensusTimestamp = 1568385900.64356000
round = 33449361, consensusTimestamp = 1568386800.3988000
round = 33451590, consensusTimestamp = 1568387700.134775000
round = 33453841, consensusTimestamp = 1568388600.287028000
round = 33456088, consensusTimestamp = 1568389500.160965000
round = 33458341, consensusTimestamp = 1568390400.1916000
round = 33460596, consensusTimestamp = 1568391300.208980000
round = 33462853, consensusTimestamp = 1568392200.327295000
round = 33465115, consensusTimestamp = 1568393100.365622000
round = 33467375, consensusTimestamp = 1568394000.302047000
round = 33469627, consensusTimestamp = 1568394900.95018000
round = 33471877, consensusTimestamp = 1568395800.161951000
round = 33474130, consensusTimestamp = 1568396700.101019000
round = 33476398, consensusTimestamp = 1568397600.347092000
round = 33478643, consensusTimestamp = 1568398500.123889000
round = 33480902, consensusTimestamp = 1568399400.263610000
round = 33483155, consensusTimestamp = 1568400300.35237000
nearestRound = round = 33483155, consensusTimestamp = 1568400300.35237000
round = 33497732, consensusTimestamp = 1568418300.443620000
round = 33499678, consensusTimestamp = 1568419200.77815000
round = 33501629, consensusTimestamp = 1568420100.66845000
round = 33503571, consensusTimestamp = 1568421000.86290000
round = 33505517, consensusTimestamp = 1568421900.205680000
round = 33507454, consensusTimestamp = 1568422800.366055000
round = 33509372, consensusTimestamp = 1568423700.88337000
round = 33511306, consensusTimestamp = 1568424600.125108000
round = 33513238, consensusTimestamp = 1568425500.626868000
round = 33515185, consensusTimestamp = 1568426400.315235000
round = 33517124, consensusTimestamp = 1568427300.213822000
round = 33519066, consensusTimestamp = 1568428200.476403000
round = 33521016, consensusTimestamp = 1568429100.331854000
round = 33522971, consensusTimestamp = 1568430000.270034000
round = 33524906, consensusTimestamp = 1568430900.243855000
round = 33526854, consensusTimestamp = 1568431800.169016000
round = 33528807, consensusTimestamp = 1568432700.171244000
round = 33530757, consensusTimestamp = 1568433600.14529000
round = 33532705, consensusTimestamp = 1568434500.315794000
round = 33534647, consensusTimestamp = 1568435400.66504000
round = 33536589, consensusTimestamp = 1568436300.406348000
round = 33538536, consensusTimestamp = 1568437200.257101000
round = 33540480, consensusTimestamp = 1568438100.278979000
round = 33542425, consensusTimestamp = 1568439000.67949000
round = 33544371, consensusTimestamp = 1568439900.334201000
round = 33546325, consensusTimestamp = 1568440800.42812000
round = 33548140, consensusTimestamp = 1568441700.503753000
round = 33550095, consensusTimestamp = 1568442600.394187000
round = 33552050, consensusTimestamp = 1568443500.345749000
round = 33554004, consensusTimestamp = 1568444400.267947000
round = 33555951, consensusTimestamp = 1568445300.410614000
round = 33557898, consensusTimestamp = 1568446200.114848000
round = 33559839, consensusTimestamp = 1568447100.385598000
round = 33561783, consensusTimestamp = 1568448000.117852000
round = 33563724, consensusTimestamp = 1568448900.329284000
round = 33565669, consensusTimestamp = 1568449800.431566000
round = 33567618, consensusTimestamp = 1568450700.205256000
round = 33569560, consensusTimestamp = 1568451600.147193000
round = 33571507, consensusTimestamp = 1568452500.25438000
round = 33573452, consensusTimestamp = 1568453400.172732000
round = 33575402, consensusTimestamp = 1568454300.244613000
round = 33577351, consensusTimestamp = 1568455200.448881000
round = 33579285, consensusTimestamp = 1568456100.541490000
round = 33581234, consensusTimestamp = 1568457000.405525000
round = 33583185, consensusTimestamp = 1568457900.101480000
round = 33585129, consensusTimestamp = 1568458800.160278000
round = 33587076, consensusTimestamp = 1568459700.147431001
round = 33589025, consensusTimestamp = 1568460600.232322000
round = 33590974, consensusTimestamp = 1568461500.153660000
round = 33592915, consensusTimestamp = 1568462400.4532000
round = 33594834, consensusTimestamp = 1568463300.125617001
round = 33596777, consensusTimestamp = 1568464200.505855000
round = 33598714, consensusTimestamp = 1568465100.61340000
round = 33600650, consensusTimestamp = 1568466000.278091000
round = 33602596, consensusTimestamp = 1568466900.50112000
round = 33604538, consensusTimestamp = 1568467800.57186000
round = 33606481, consensusTimestamp = 1568468700.341131001
round = 33608415, consensusTimestamp = 1568469600.272420000
round = 33610339, consensusTimestamp = 1568470500.232173003
round = 33612274, consensusTimestamp = 1568471400.395255002
round = 33614201, consensusTimestamp = 1568472300.419370000
round = 33616125, consensusTimestamp = 1568473200.97538000
round = 33618024, consensusTimestamp = 1568474100.155924000
round = 33619959, consensusTimestamp = 1568475000.268261001
round = 33621900, consensusTimestamp = 1568475900.344629000
round = 33623848, consensusTimestamp = 1568476800.223327000
round = 33625795, consensusTimestamp = 1568477700.405208001
round = 33627735, consensusTimestamp = 1568478600.160141000
round = 33629676, consensusTimestamp = 1568479500.12981000
round = 33631622, consensusTimestamp = 1568480400.229538000
round = 33633563, consensusTimestamp = 1568481300.382612000
round = 33635505, consensusTimestamp = 1568482200.215566000
round = 33637450, consensusTimestamp = 1568483100.12618000
round = 33639397, consensusTimestamp = 1568484000.148537000
round = 33641336, consensusTimestamp = 1568484900.168171000
round = 33643268, consensusTimestamp = 1568485800.153314000
round = 33645207, consensusTimestamp = 1568486700.374032000
round = 33647158, consensusTimestamp = 1568487600.334885000
round = 33649120, consensusTimestamp = 1568488500.389754000
round = 33651070, consensusTimestamp = 1568489400.213536000
round = 33653023, consensusTimestamp = 1568490300.244015000
round = 33654970, consensusTimestamp = 1568491200.162668000
round = 33656919, consensusTimestamp = 1568492100.74632001
round = 33658857, consensusTimestamp = 1568493000.313905000
round = 33660787, consensusTimestamp = 1568493900.48890000
round = 33662732, consensusTimestamp = 1568494800.4828001
round = 33664681, consensusTimestamp = 1568495700.342410000
round = 33666630, consensusTimestamp = 1568496600.342000000
round = 33668579, consensusTimestamp = 1568497500.305262000
round = 33670523, consensusTimestamp = 1568498400.4091000
round = 33672473, consensusTimestamp = 1568499300.105546000
round = 33674413, consensusTimestamp = 1568500200.87029000
round = 33676367, consensusTimestamp = 1568501100.218160000
round = 33678308, consensusTimestamp = 1568502000.61027000
round = 33680258, consensusTimestamp = 1568502900.272189000
round = 33682203, consensusTimestamp = 1568503800.34171001
 */
