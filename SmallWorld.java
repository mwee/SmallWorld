/*
  CS 61C Project1: Small World

  Name: Nelson Zhang
  Login: cs61c-kg

  Name: Michael Wee
  Login: cs61c-it
 */

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.SequenceFile;

public class SmallWorld {
	// Maximum depth for any breadth-first search
	public static final int MAX_ITERATIONS = 20;

	// Skeleton code uses this to share denom cmd-line arg across cluster
	public static final String DENOM_PATH = "denom.txt";

	// Counter for visited vertices in each bfs iteration
	public static enum ValueUse {
		VISITED
	};

	
	/** WRITABLE CLASSES **/
	
	// Custom Writable subclass, contains information about children
	// and which searches have passed/are passing through the vertex.
	public static class StateWritable implements Writable {
		public HashMap<Long, Boolean> visitedBy; // sourceVertex: true/false (frontier/visited)
		public ArrayList<Long> children; // list of children, built during FormatReduce

		public StateWritable(HashMap<Long, Boolean> visitedBy, ArrayList<Long> children) {
			this.visitedBy = visitedBy;
			this.children = children;
		}

		public StateWritable(HashMap<Long, Boolean> visitedBy) {
    		this(visitedBy, new ArrayList<Long>());
    	}

		public StateWritable(ArrayList<Long> children) {
			this(new HashMap<Long, Boolean>(), children);
		}
		
		public StateWritable() {
			this(new HashMap<Long, Boolean>(), new ArrayList<Long>());
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(children.size());
			for (int i=0; i<children.size(); i++) {
				out.writeLong(children.get(i));
			}
			out.writeLong(visitedBy.size());
			for (long key : visitedBy.keySet()) {
				out.writeLong(key);
				out.writeBoolean(visitedBy.get(key));
			}
		}

		public void readFields(DataInput in) throws IOException {
    		long size = in.readLong();
    		this.children = new ArrayList<Long>();
    		this.visitedBy = new HashMap<Long, Boolean>();
    		for (int i=0; i<size; i++) {
    			this.children.add(in.readLong());
    		}
    		size = in.readLong();
    		for (int i=0; i<size; i++) {
    			this.visitedBy.put(in.readLong(), in.readBoolean());
    		}
    	}

		public String toString() {
			return "(" + visitedBy.toString() + ", " + children.toString() + ")";
		}
	}

	
	
	/** MAPREDUCE CLASSES **/

	// Format mapper: move along, nothing to see here.
	public static class FormatMap extends
			Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void map(LongWritable key, LongWritable child, Context context)
                throws IOException, InterruptedException {
        	context.write(key, child);
        }
	}

	
	// Format reducer: converts values to the (children, history) 
	// and randomly selects root nodes
	public static class FormatReduce extends
			Reducer<LongWritable, LongWritable, LongWritable, StateWritable> {
		public long denom;

		// Load the cached denom value
		@Override
		public void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				Path cachedDenomPath = DistributedCache
						.getLocalCacheFiles(conf)[0];
				BufferedReader reader = new BufferedReader(new FileReader(
						cachedDenomPath.toString()));
				String denomStr = reader.readLine();
				reader.close();
				denom = Long.decode(denomStr);
			} catch (IOException ioe) {
				System.err.println("IOException reading denom from distributed cache");
				System.err.println(ioe.toString());
			}
		}

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			ArrayList<Long> children = new ArrayList<Long>();
			HashMap<Long, Boolean> visitedBy = new HashMap<Long, Boolean>();
			// At random, tag this vertex as frontier in a search 
			// starting from itself, i.e. it is a root vertex.
			if (Math.random() < 1.0 / denom) {
				visitedBy.put(key.get(), true);
				context.getCounter(ValueUse.VISITED).increment(1);
			}
			for (LongWritable child : values) {
				children.add(child.get());
			}
			context.write(key, new StateWritable(visitedBy, children));
		}
	}

	
	// BFS mapper: advance the frontier by one level, and tag vertices accordingly
	public static class BFSMap extends
			Mapper<LongWritable, StateWritable, LongWritable, StateWritable> {
		public void map(LongWritable src, StateWritable value, Context context)
				throws IOException, InterruptedException {
			HashMap<Long, Boolean> visitedBy = new HashMap<Long, Boolean>();
			// Each child vertex inherits the frontier states w.r.t. each root
			for (long key : value.visitedBy.keySet()) {
				if (value.visitedBy.get(key)) {
					visitedBy.put(key, true);
					value.visitedBy.put(key, false);
				}
			}
			// Advance the frontier
			StateWritable inherited = new StateWritable(visitedBy);
			for (long child : value.children) {
				context.write(new LongWritable(child), inherited);
			}
			// Also pass on the input kv-pair, so reducer can receive
			// information about each vertex's children
			context.write(src, value);
		}
	}

	
	// BFS reducer: Gets a newly advanced frontier vertex and its inherited
	// frontier states, as well as the list of children for this vertex.
	public static class BFSReduce extends
			Reducer<LongWritable, StateWritable, LongWritable, StateWritable> {
		public void reduce(LongWritable src, Iterable<StateWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<Long, Boolean> visitedBy = new HashMap<Long, Boolean>();
			ArrayList<Long> children = new ArrayList<Long>();
			long frontiers = 0;
			// Vertex could have been advanced into from multiple parent vertices,
			// so combine the frontier states.
			for (StateWritable value : values) {
				if (value.children.size() > 0) children = value.children;
				for (long key : value.visitedBy.keySet()) {
					if (visitedBy.containsKey(key)) {
						// If there exists a false value for the frontier state w.r.t. a
						// root node, then it must have visited the vertex already, and it
						// should not be a frontier vertex!
						visitedBy.put(key, (visitedBy.get(key) && value.visitedBy.get(key)));
					}
					else visitedBy.put(key, value.visitedBy.get(key));
				}
			}
			// Count the number of searches for which this is a frontier vertex
			for (long key : visitedBy.keySet()) {
				if (visitedBy.get(key)) frontiers++; 
			}
			context.write(src, new StateWritable(visitedBy, children));
			context.getCounter(ValueUse.VISITED).increment(frontiers); // Add to the distance count
		}
	}

	
	// Shares denom argument across the cluster via DistributedCache
	public static void shareDenom(String denomStr, Configuration conf) {
		try {
			Path localDenomPath = new Path(DENOM_PATH + "-source");
			Path remoteDenomPath = new Path(DENOM_PATH);
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					localDenomPath.toString()));
			writer.write(denomStr);
			writer.newLine();
			writer.close();
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(true, true, localDenomPath, remoteDenomPath);
			DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
		} catch (IOException ioe) {
			System.err.println("IOException writing to distributed cache");
			System.err.println(ioe.toString());
		}
	}

	
	public static void main(String[] rawArgs) throws Exception {
		GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
		Configuration conf = parser.getConfiguration();
		String[] args = parser.getRemainingArgs();

		// Set denom from command line arguments
		shareDenom(args[2], conf);
		
		// Setting up mapreduce job to load in graph
		Job job = new Job(conf, "load graph");
		job.setJarByClass(SmallWorld.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(StateWritable.class);

		job.setMapperClass(FormatMap.class);
		job.setReducerClass(FormatReduce.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Input from command-line argument, output to predictable place
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

		// Actually starts job, and waits for it to finish
		job.waitForCompletion(true);
		
		// Create the histogram, and insert the # of root vertices (distance 0)
		ArrayList<Long> histogram = new ArrayList<Long>();
		long visited = job.getCounters().findCounter(ValueUse.VISITED).getValue();
		histogram.add(visited);

		// Repeats your BFS mapreduce
		int i = 0;
		while (i < MAX_ITERATIONS) {
			job = new Job(conf, "bfs" + i);
			job.setJarByClass(SmallWorld.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(StateWritable.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(StateWritable.class);

			job.setMapperClass(BFSMap.class);
			job.setReducerClass(BFSReduce.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			// Notice how each mapreduce job gets gets its own output dir
			FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
			FileOutputFormat.setOutputPath(job, new Path("bfs-" + (i + 1)
					+ "-out"));

			job.waitForCompletion(true);
			
			// Get the # of vertices at this distance
			visited = job.getCounters().findCounter(ValueUse.VISITED).getValue();
			if (visited == 0) break; // if none visited, graph has been fully traversed!
			
			// Update the histogram
			histogram.add(visited);
			i++;
		}

		// Hapoop the histogram into a file.
		BufferedWriter out = new BufferedWriter(new FileWriter("OUTPUT.txt"));
		for (int j=0; j<histogram.size(); j++) {
			out.write("Dist " + j + ": " + histogram.get(j) + " vertices\n");
			System.out.println("Dist " + j + ": " + histogram.get(j) + " vertices");
		}
		out.close();
	}
}
