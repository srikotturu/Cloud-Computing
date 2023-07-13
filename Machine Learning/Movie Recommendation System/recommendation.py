from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

if __name__ == "__main__":
    sc = SparkContext(appName="PythonCollaborativeFilteringExample")  # Initialize SparkContext

    # Load and parse the data
    data = sc.textFile("gs://your-bucket-name/converted_data.txt")  # Load the converted data file from GCS bucket

    ratings = data.map(lambda l: l.split(','))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))  # Parse the data into Rating objects

    rank = 10  # Set the rank (number of features) for ALS
    numIterations = 10  # Set the number of iterations for ALS

    model = ALS.train(ratings, rank, numIterations)  # Build the recommendation model using ALS

    # Evaluate the model and calculate Mean Squared Error (MSE)
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    # Save and load model (optional)
    model.save(sc, "gs://your-bucket-name/myCollaborativeFilter")  # Save the model to GCS bucket
    sameModel = MatrixFactorizationModel.load(sc, "gs://your-bucket-name/myCollaborativeFilter")  # Load the saved model from GCS bucket
