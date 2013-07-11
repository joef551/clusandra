package clusandra.stream;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import java.io.Serializable;
import java.util.Random;
import moa.core.InstancesHeader;
import moa.core.ObjectRepository;
import moa.options.AbstractOptionHandler;
import moa.options.IntOption;
import moa.streams.InstanceStream;
import moa.tasks.TaskMonitor;

public class RandomRbfGenerator extends AbstractOptionHandler implements
		InstanceStream {

	@Override
	public String getPurposeString() {
		return "Generates a random radial basis function stream.";
	}

	private static final long serialVersionUID = 1L;

	// The number of classes to generate
	public IntOption numClassesOption = new IntOption("numClasses", 'c',
			"The number of classes to generate.", 5);

	// The number of attributes
	public IntOption numAttsOption = new IntOption("numAtts", 'a',
			"The number of attributes to generate.", 6);

	// This represents a centroid from which instances are produced
	protected static class Centroid implements Serializable {
		private static final long serialVersionUID = 1L;
		double[] centre;
		int classLabel;
		double stdDev;
	}

	protected InstancesHeader streamHeader;

	// The collection of centroids
	protected Centroid[] centroids;

	// The random number generator used for creating centroids and instances
	protected Random instanceRandom = new Random();

	@Override
	public void prepareForUseImpl(TaskMonitor monitor,
			ObjectRepository repository) {
		monitor.setCurrentActivity("Preparing random RBF...", -1.0);
		generateHeader();
		generateCentroids();
	}

	public InstancesHeader getHeader() {
		return this.streamHeader;
	}

	public long estimatedRemainingInstances() {
		return -1;
	}

	public boolean hasMoreInstances() {
		return true;
	}

	public boolean isRestartable() {
		return true;
	}

	public void restart() {
		instanceRandom = new Random();
	}

	/**
	 * Create a new instance from the bucket of centroids.
	 */
	public Instance nextInstance() {

		// First choose a centroid, at random, from which to produce an instance
		Centroid centroid = centroids[instanceRandom.nextInt(numClassesOption
				.getValue())];

		// Create a random vector
		int numAtts = numAttsOption.getValue();
		// extra attribute is for the class label
		double[] attVals = new double[numAtts + 1];
		for (int i = 0; i < numAtts; i++) {
			// choose a random number between -1 and 1.
			//attVals[i] = (instanceRandom.nextDouble() * 2.0) - 1.0;
			attVals[i] = instanceRandom.nextGaussian();
		}

		// next, find its magnitude or length
		double magnitude = 0.0;
		for (int i = 0; i < numAtts; i++) {
			magnitude += Math.pow(attVals[i], 2);
		}
		magnitude = Math.sqrt(magnitude);

		double desiredMag = instanceRandom.nextGaussian() * centroid.stdDev;
		//double desiredMag = instanceRandom.nextDouble() * centroid.stdDev;

		double scale = desiredMag / magnitude;
		for (int i = 0; i < numAtts; i++) {
			attVals[i] = centroid.centre[i] + (attVals[i] * scale);
		}
		Instance inst = new DenseInstance(1.0, attVals);
		inst.setDataset(getHeader());
		inst.setClassValue(centroid.classLabel);
		return inst;
	}

	public Instance nextInstanceX() {
		boolean found = false;
		double[] attVals = new double[numAttsOption.getValue() + 1];
		int closestCentroid = -1;
		// System.out.println("Number of centroids = " + centroids.length);
		do {
			// first, create a random vector
			for (int i = 0; i < attVals.length; i++) {
				attVals[i] = instanceRandom.nextGaussian();
			}
			// next, find the centroid that it is closest to
			double closestDistance = Double.MAX_VALUE;
			for (int i = 0; i < centroids.length; i++) {
				double distance = getDistance(centroids[i].centre, attVals);
				if (distance < closestDistance) {
					closestDistance = distance;
					closestCentroid = i;
				}
			}
			if (getDistance(centroids[closestCentroid].centre, attVals) <= (centroids[closestCentroid].stdDev)) {
				found = true;
			}
		} while (!found);
		// System.out.println("Closest centroid = " + closestCentroid);
		// System.out.println("Class label = " +
		// centroids[closestCentroid].classLabel);
		Instance inst = new DenseInstance(1.0, attVals);
		inst.setDataset(getHeader());
		inst.setClassValue(centroids[closestCentroid].classLabel);
		return inst;
	}

	protected void generateHeader() {
		FastVector attributes = new FastVector();
		for (int i = 0; i < this.numAttsOption.getValue(); i++) {
			attributes.addElement(new Attribute("att" + (i + 1)));
		}
		FastVector classLabels = new FastVector();
		for (int i = 0; i < this.numClassesOption.getValue(); i++) {
			classLabels.addElement("class" + (i + 1));
		}
		attributes.addElement(new Attribute("class", classLabels));
		this.streamHeader = new InstancesHeader(new Instances(
				getCLICreationString(InstanceStream.class), attributes, 0));
		this.streamHeader.setClassIndex(this.streamHeader.numAttributes() - 1);
	}

	protected void generateCentroids() {

		// System.out.println("generateCentroids: started");

		// create our random number generator
		Random modelRand = new Random();
		// Create the array of centroids
		centroids = new Centroid[numClassesOption.getValue()];
		for (int i = 0; i < centroids.length; i++) {
			centroids[i] = null;
		}


		// Create the Centroids
		for (int i = 0; i < centroids.length; i++) {

			centroids[i] = new Centroid();
			centroids[i].classLabel = -1;
			// Create the actual center for this Centroid
			// The centroids should be a certain distance from each other
			// We don't want them up close to one another.
			boolean goodCentroid = true;
			double[] center = new double[numAttsOption.getValue()];
			do {
				for (int j = 0; j < center.length; j++) {
					// choose a random gaussian number 
					center[j] = modelRand.nextGaussian();
					//center[j] = (modelRand.nextDouble() * 2.0) - 1.0;
				}
				goodCentroid = true;
				// make sure that this center is a proper distance from all
				// the other existing centers
				for (int j = 0; j < centroids.length && goodCentroid; j++) {
					if (j != i && centroids[j] != null) {
						if (getDistance(center, centroids[j].centre) <= (centroids[j].stdDev * 7.5)) {
							goodCentroid = false;
						}
					}
				}
			} while (!goodCentroid);
			centroids[i].centre = center;

			// make sure we don't duplicate labels
			boolean goodLabel;
			int label = -1;
			do {
				goodLabel = true;
				label = modelRand.nextInt(numClassesOption.getValue());
				for (int j = 0; j < centroids.length && goodLabel; j++) {
					if (j != i && centroids[j] != null
							&& label == centroids[j].classLabel) {
						goodLabel = false;
					}
				}
			} while (!goodLabel);
			centroids[i].classLabel = label;
			centroids[i].stdDev = modelRand.nextDouble();
		}
	}

	/**
	 * Get the deistance between two vectors
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static double getDistance(double[] v1, double[] v2) {
		double res = 0.0;
		for (int i = 0; i < v1.length; i++) {
			res += Math.pow((v1[i] - v2[i]), 2);
		}
		return Math.sqrt(res);
	}

	public void getDescription(StringBuilder sb, int indent) {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {
		System.out.println("Starting");
		RandomRbfGenerator gen = new RandomRbfGenerator();
		gen.prepareForUse();
		gen.restart();
		gen.nextInstance();
		System.out.println("Completed");
	}

}
