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
  <a href="#validation-process">Validation process</a> •
  <a href="#authors-es-blue_heart-it">Authors</a> •
  <a href="#license">License</a>
 </p>

## Getting started

The easiest way to run this project is by cloning the project locally, create a fat jar using Maven and executing the shell
script that can be found on the project's root directory.

```bash
mvn clean package
./run.sh
```

It is possible to active/deactivate the explore stage with the `--explore` flag (add/remove this flag inside the 
`run.sh` script).

The output should be similar to the following one:

![project-demo](https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/sparkml-flights-delay-demo.gif?raw=true)

You can also import it to your favourite IDE, but keep in mind that the program requires one argument, which is the dataset
to process. You can find multiple valid datasets at this link: [Airline On-Time Statistics and Delay Causes](http://stat-computing.org/dataexpo/2009/the-data.html).

Be aware that it can take a lot of time with a large dataset (14 models are trained with 10 folds cross-validation).
This is why we included a small `tuning.csv` file in the `raw` folder. Please, consider using this dataset to check
that the program works properly.

## Validation process

The general workflow on the program is shown in the image below:

![project-flow](https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/sparkml-flights-delay-flow.png?raw=true)

Hyperparameter tuning and model selection are carried out using cross-validation on the training dataset. In this stage,
a grid search is performed using two different models: Linear Regression and Random Forest (you can add your own 
extending the `CVTuningPipeline` class). Finally, the test error of the best model is obtained using the test set.

## Authors :es: :blue_heart: :it:

* **Fernando Díaz**
* **Giorgio Ruffa**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
