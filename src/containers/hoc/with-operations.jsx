import React from "react";
import { graphql, compose, withApollo } from "react-apollo";
import { withProps, branch, renderComponent } from "recompose";

import { Card, CardHeader, CardText } from "material-ui/Card";

import LoadingIndicator from "../../components/LoadingIndicator";

/**
 * This HOC takes a list of GraphQL query names and adds a loading prop that is true if any of the
 * queries are loading.
 * @param {string[]} queryNames The names of the queries to check loading state
 */
const isLoading = queryNames =>
  withProps(parentProps => {
    const loadingReducer = (loadingAcc, queryName) =>
      loadingAcc || (parentProps[queryName] || {}).loading;
    const loading = queryNames.reduce(loadingReducer, false);

    const errorReducer = (errorAcc, queryName) => {
      const error = (parentProps[queryName] || {}).error;
      return error ? errorAcc.concat([error]) : errorAcc;
    };
    const errors = queryNames.reduce(errorReducer, []);

    return { loading, errors };
  });

export const withQueries = (queries = {}) => {
  const enhancers = Object.entries(queries).map(
    ([name, { query: queryGql, ...config }]) =>
      graphql(queryGql, { ...config, name })
  );

  return compose(
    ...enhancers,
    isLoading(Object.keys(queries))
  );
};

export const withMutations = (mutations = {}) =>
  compose(
    withApollo,
    withProps(parentProps => {
      const reducer = (propsAcc, [name, constructor]) => {
        propsAcc[name] = async (...args) => {
          const options = constructor(parentProps)(...args);
          return await parentProps.client.mutate(options);
        };
        return propsAcc;
      };

      const mutationFuncs = Object.entries(mutations).reduce(reducer, {});
      return { mutations: mutationFuncs };
    })
  );

/**
 * Takes multiple GraphQL queriy and/or mutation definitions and wraps Component in appropriate
 * graphql() calls.
 */
export const withOperations = options => {
  const { queries = {}, mutations = {} } = options;
  return compose(
    withQueries(queries),
    withMutations(mutations)
  );
};

// remove 'GraphQL Error:' from error messages, per client request
export const formatErrorMessage = error => {
  return error.message.replaceAll("GraphQL Error:", "").trim();
};

export const PrettyErrors = ({ errors }) => {
  return (
    <Card style={{ margin: "10px" }}>
      <CardHeader title="Encountered errors" />
      <CardText>
        <ul>
          {errors.map((err, index) => {
            return <li key={index}>{formatErrorMessage(err.message)}</li>;
          })}
        </ul>
      </CardText>
    </Card>
  );
};

/**
 * Similar to {@link withOperations}, but shows a loading indicator if any of the queries are loading.
 *
 * @param {Object} options
 * @see withOperations
 */
export const loadData = options =>
  compose(
    withOperations(options),
    branch(({ loading }) => loading, renderComponent(LoadingIndicator)),
    branch(({ errors }) => errors.length > 0, renderComponent(PrettyErrors))
  );
