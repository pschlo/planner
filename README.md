# planner

Dependency injection framework for creating execution plans.

## Installation
This package is not currently uploaded to PyPI. Install as follows:

1. Find your release of choice [here](https://github.com/pschlo/planner/releases)
2. Copy the link to `planner-x.x.x.tar.gz`
3. Run `python -m pip install {link}`

You may also prepend a [direct reference](https://peps.python.org/pep-0440/#direct-references), which might be desirable for a `requirements.txt`.


## Building
The `.tar.gz` file in a release is the [source distribution](https://packaging.python.org/en/latest/glossary/#term-Source-Distribution-or-sdist), which was created from the source code with `uv build --sdist`. [Built distributions](https://packaging.python.org/en/latest/glossary/#term-Built-Distribution) are not provided.

## Overview

* An `Asset` is an (intermediate) result, representing a completed computation step
* Each `Asset` can be created ("made") by zero or more `Recipe`s
* The `Asset` is the **what**, the `Recipe` is the **how**
* A `Recipe` may depend on zero or more `Asset`s, which it can treat as given and use in its `make()` method to produce its `Asset`
* This allows for a very modular design; for instance, a specific simulation can be represented as a `SimulationResultAsset`, which is made with a `SimulationResultRecipe`, which depends on some other `Asset`s like `ConfigurationAsset`. The `SimulationResultRecipe` does not need to care about **how** the `Asset`s are made, it can just take them as given and use them to produce its `SimulationResultAsset`.
* When the simulation should be executed (i.e., the target `Asset` should be made), `Recipe`s for the target asset and all intermediate assets must be present
* For this, a `Planner` can be created, to which `Recipe`s may be added in any order
* The Planner may finally be compiled into a `Plan` by resolving the `Asset` / `Recipe` dependencies
* This `Plan` may then be executed to produce the target `Asset`. During this, the required `Recipe`s are executed in a correct order.
* This means that for a simulation, the behavior or the input data can easily be adapted by simply providing different `Recipe`s. As long as the produced `Asset`s are correct, the simulation will work and must not be aware of e.g. the changed inputs.
