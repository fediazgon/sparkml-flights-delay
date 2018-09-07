<h1 align="center">
  <div style="margin:10px;">
    <img src="https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/sparkml-flights-delay-logo.png?raw=true" alt="project-logo" width="200px">
  </div>
  sparkml-flights-delay
</h1>

<h4 align="center">
Predicting the arrival delay time of a commercial flights using Apache Spark <a href="https://spark.apache.org/mllib/">MLlib</a>
</h4>

<p align="center">
  <a href="#getting-started">Getting started</a> •
  <a href="#validation">Validation</a> •
  <a href="#authors">Authors</a> •
  <a href="#license">License</a>
 </p>

## Getting started

The easiest way to run this project is cloning the project locally, create a fat jar using Maven and executing the shell
script that can be found on the project's root directory.

```bash
mvn clean package
./run.sh
```

The output should be similar to the following one:

![project-demo](https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/sparkml-flights-delay-demo.gif?raw=true)

You can also import it to your favorite IDE, but keep in mind that the program requires one argument, which is the dataset
to process. You can find multiple valid datasets at this link: [Airline On-Time Statistics and Delay Causes](http://stat-computing.org/dataexpo/2009/the-data.html).

Be aware that some stages can take a lot of time with a large dataset (14 models are trained with 10 folds cross validation).
This is why we included a small `tuning.csv` file in the `raw` folder. Please, consider to use this dataset if you want to
run all the stages. These stages are triggered using the following flags (add/remove them inside the `run.sh` script):

* `--explore`: compute some statistics on the dataset (e.g., flights by carrier, cancelled flights).
* `--tune`: obtain the best set of parameters (e.g., elasticNetParam, regParam) for different algorithms (Linear Regression and Random Forest).
* `--compare`: compare different algorithms using the best parameters from the tuning stage.

## Validation process

**Work pending**

Reviewing the workflow of the program we have found some issues in the validation process. For example, during the `tune`
stage, a grid search is carried out on the whole dataset. We should have split the dataset into training/validation/test from the very
beginning.

## Authors :es: :blue_heart: :it:

* **Fernando Díaz**
* **Giorgio Ruffa**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
