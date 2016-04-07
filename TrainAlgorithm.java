package de.uni_ulm.omi.hasan.alertEngine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import utils.DataSetFilter;
import utils.GetFilePath;
import utils.MetricNames;
import utils.PlotGraph;
import utils.ReadCSVIntoMemory;

/**
 * This class is responsible for training Machine Learning algorithm from Dataset.
 * 
 * @author Syed Tauqeer Hasan <syed.hasan@uni-ulm.de>
 */
public class TrainAlgorithm {

	private static MetricNames mMetricName;

	/**
	 * Trains the algorithm with KMean Clustering.
	 * 
	 * @param meanAverageFiler boolean filter which identifies whether to use mean average filter or not
	 */
	@Deprecated
	public static void trainKMeanClustering(final boolean meanAverageFilter){

		// Spark Context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("kMeanClustering");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parse the data from CSV to RDD<Vector>
		List<Double> realTimeList = ReadCSVIntoMemory.parseSingleDimensionalDataToRDD(MetricNames.CPU_USAGE, sc, true );

		// Parsing the List<Double> into JavaRDD<Double>.
		JavaRDD<Double> parsedData = sc.parallelize(realTimeList);

		// Mapping the JavaRDD<Double> to JavaRDD<Vector> required for the ML Algorithm.
		JavaRDD<Vector> parsedVector = parsedData.map(
				new Function<Double, Vector>() {
					public Vector call(Double s) {
						return Vectors.dense(s);
					}});
		// Caching the Vector.
		parsedVector.cache();

		final int clustersNumbers = 2;
		final int iterations = 100;


		// Training the KMean model using Dataset.
		KMeansModel clusters = KMeans.train(parsedVector.rdd(), clustersNumbers, iterations);

		/**
		 *  Evaluate clustering by computing Within Set Sum of Squared Errors
		 */
		double WSSSE = clusters.computeCost(parsedVector.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
		System.out.println("Number of Clusters: " + clusters.clusterCenters().length);

		Vector[] clusPoints = clusters.clusterCenters();
		System.out.println("Center point of first cluster: " + clusPoints[0]);
		//		System.out.println("Center point of second cluster: " + clusPoints[1]);
		//		System.out.println("Center point of second cluster: " + clusPoints[2]);

		// Clearing cache and closing Spark Context.
		parsedVector.unpersist();
		sc.close();
	}

	/**
	 * Trains the Gaussian Mixture Model.
	 * 
	 * @param metricName Metric for which the system is to be trained.
	 * @param meanAverageFiler boolean filter which identifies whether to use mean average filter or not.
	 * @return list of arrays containing the mean, variance and probability of each cluster.
	 */
	@SuppressWarnings("serial")
	public static List<double[]> trainGMM(final MetricNames metricName, final Boolean meanAverageFilerFlag){

		mMetricName = metricName;

		/**
		 * TODO: Plotting the dataset for proper visualization. Will be removed later.
		 */
		// Accessing the path to the CSV file.
		final String csvPath = GetFilePath.getTrainingDataSetPath(metricName);
		ArrayList<ArrayList<String>> csvData = ReadCSVIntoMemory.readCsvIntoMemory(csvPath);
		PlotGraph.plotGraph(metricName, csvData);

		/**
		 * End of plotting part
		 */

		SparkConf conf = new SparkConf().setMaster("local").setAppName("GaussianMixture");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parse the data from CSV to RDD<Vector>
		List<Double> parsedList = ReadCSVIntoMemory.parseSingleDimensionalDataToRDD(metricName, sc, meanAverageFilerFlag);

		// Parsing the List<Double> into JavaRDD<Double>.
		JavaRDD<Double> parsedData = sc.parallelize(parsedList);

		// Mapping the JavaRDD<Double> to JavaRDD<Vector> required for the ML Algorithm.
		JavaRDD<Vector> parsedVector = parsedData.map(
				new Function<Double, Vector>() {
					public Vector call(Double s) {
						return Vectors.dense(s);
					}});

		// Caching the Vector.
		parsedVector.cache();

		// If datapoints to be normalized between 0 and 1.
		//		parsedVector = StatisticalOperation.normalizeData(parsedVector);

		/**
		 * For extracting the mean and variance of distribution.
		 */
		MultivariateStatisticalSummary summary1 = Statistics.colStats(parsedVector.rdd());

		// Removing the vector from cache because a new vector has to be created on the basis of outlier filtered.
		parsedVector.unpersist();

		/**
		 * Outlier filtering of the input dataset.
		 */
		parsedList = DataSetFilter.filterOutlierDataPointsAndAddNoise(mMetricName, parsedList, summary1.mean().toArray()[0],
				summary1.variance().toArray()[0]);

		/**
		 * TODO plotting of filtered data, to be removed.
		 */
		ArrayList<String> realTimeDataString = new ArrayList<String>();
		ArrayList<ArrayList<String>> completeDataString = new ArrayList<ArrayList<String>>();
		for (double x : parsedList){
			realTimeDataString.add(Double.toString(x));
		}
		completeDataString.add(realTimeDataString);
		PlotGraph.plotGraph(metricName, completeDataString);

		/**
		 * Creating a new vector from the filtered list.
		 */
		parsedData = sc.parallelize(parsedList);

		// Mapping the JavaRDD<Double> to JavaRDD<Vector> required for the ML Algorithm.
		parsedVector = parsedData.map(
				new Function<Double, Vector>() {
					public Vector call(Double s) {
						return Vectors.dense(s);
					}});

		// Caching the fitered Vector.
		parsedVector.cache();

		/**
		 * FOR CPU_IDLE: Clusters = 2 : if 2 gaussians in the dataset: CPU_IDLE shifts to a new mean if workload changes and then is stable
		 * FOR MEMORY_IDLE: Clusters = 2 : memory is normally stable: so use 2 clusters so that the stable data are captured in the 1 noisy clusters and main in one
		 * FOR DISK_READ: Clusters = 5: very fluctuating data, the more the clusters the better the result
		 */
		int clustersNum = 0;

		if(metricName == MetricNames.DISK_WRITE || metricName == MetricNames.DISK_READ){
			clustersNum = 5;
		} else {
			clustersNum = 3;
		}

		//Traing the ML Model
		GaussianMixtureModel gmm = new GaussianMixture().setK(clustersNum).run(parsedVector.rdd());

		//		GaussianMixtureModel gmm = new GaussianMixture().run(parsedVector.rdd());

		/**
		 * List of array, going to contain the mean, standard deviation and probability of each cluster.
		 * For each array, index 0 represents the cluster probability, index 1 represents mean,
		 * and index 2 represents the standard deviation.
		 */
		List<double[]> clustersParameters = DataSetFilter.filterOutlierClusters(metricName, gmm, summary1);

		// Number of clusters returned by the training algorithm.
		final int clustersRemaining = clustersParameters.size();

		// Variable for containing the sum of all cluster probabilities
		double probSum = 0;

		// Normalizing the cluster probabilities
		for(int j = 0; j < clustersRemaining; j++){
			probSum = probSum + clustersParameters.get(j)[0];
		}

		// Iterating through each cluster and calculating the weighted mean & SD.
		for(int i = 0; i< clustersRemaining; i++) {
			clustersParameters.get(i)[0] = (clustersParameters.get(i)[0]) / probSum ;
		}

		// Saving the parameter on disk so that it can be used for anomaly detection.
		saveTrainingParametersOnDisk(clustersParameters);

		parsedVector.unpersist();
		sc.close();

		return clustersParameters;
	}


	/**
	 * Method for storing training parameters on disk, to be used by other modules.
	 * @param params Parameters to be stored on disk.
	 */
	private static void saveTrainingParametersOnDisk(List<double[]> params){

		// Accessing the path to the parameter file.
		final String paraMeterPath = GetFilePath.getParameterPath(mMetricName);

		// Deleting the saved parameters if it already exists.
		File paramFile = new File(paraMeterPath);
		if (paramFile.exists()) {
			paramFile.delete();
		}

		/**
		 * STORING PARAMETERS OF TRAINING ON DISK TO BE USED BY OTHER MODULES
		 */
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(paraMeterPath));
			oos.writeObject(params);
			oos.flush();
			oos.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

}
