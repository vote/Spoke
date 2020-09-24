import PropTypes from "prop-types";
import React from "react";
import gql from "graphql-tag";
import { withRouter } from "react-router";
import { compose } from "react-apollo";

import { RaisedButton } from "material-ui";
import Check from "material-ui/svg-icons/action/check-circle";

import { loadData } from "./hoc/with-operations";
import Empty from "../components/Empty";
import TexterRequest from "../components/TexterRequest";
import AssignmentSummary from "../components/AssignmentSummary";

const dueBySortFn = (x, y) => (x.dueBy > y.dueBy ? -1 : 1);
const needSortFn = (x, y) =>
  x.unmessagedCount + x.unrepliedCount > y.unmessagedCount + y.unrepliedCount
    ? -1
    : 1;
const SORT_METHOD = dueBySortFn;

// Add env vars
const ASSIGNMENT_REQUEST_BLOCK_UNMESSAGED = 2000;
const ASSIGNMENT_REQUEST_BLOCK_UNREPLIED = 50;

class TexterTodoList extends React.Component {
  state = {
    releasingReplies: false,
    releasedReplies: false,
    releasedRepliesError: undefined
  };

  canRequestContacts(assignments) {
    const unmessagedCount = assignments
      .map(a => a.unmessagedCount)
      .reduce((x, y) => x + y, 0);
    const unrepliedCount = assignments
      .map(a => a.unrepliedCount)
      .reduce((x, y) => x + y, 0);
    return !(
      unmessagedCount > ASSIGNMENT_REQUEST_BLOCK_UNMESSAGED
      || unrepliedCount > ASSIGNMENT_REQUEST_BLOCK_UNREPLIED
    );
  }

  renderTodoList(assignments) {
    const { organizationId } = this.props.match.params;
    return assignments
      .sort()
      .map(assignment => {
        if (
          assignment.unmessagedCount > 0 ||
          assignment.unrepliedCount > 0 ||
          assignment.badTimezoneCount > 0 ||
          assignment.campaign.useDynamicAssignment ||
          assignment.pastMessagesCount > 0 ||
          assignment.skippedMessagesCount > 0
        ) {
          return (
            <AssignmentSummary
              organizationId={organizationId}
              key={assignment.id}
              assignment={assignment}
              unmessagedCount={assignment.unmessagedCount}
              unrepliedCount={assignment.unrepliedCount}
              badTimezoneCount={assignment.badTimezoneCount}
              totalMessagedCount={assignment.totalMessagedCount}
              pastMessagesCount={assignment.pastMessagesCount}
              skippedMessagesCount={assignment.skippedMessagesCount}
            />
          );
        }
        return null;
      })
      .filter(ele => ele !== null);
  }
  componentDidMount() {
    this.props.data.refetch();
    // re-asserts polling after manual refresh
    // this.props.data.startPolling(5000)
  }

  termsAgreed() {
    const { data, history } = this.props;
    if (window.TERMS_REQUIRE && !data.currentUser.terms) {
      history.push(`/terms?next=${this.props.location.pathname}`);
    }
  }

  releaseMyReplies = () => {
    this.setState({
      releasingReplies: true
    });

    this.props.mutations
      .releaseMyReplies(this.props.match.params.organizationId)
      .then(() => {
        this.setState({ releasingReplies: false, releasedReplies: true });
      })
      .catch(error => {
        this.setState({ releasingReplies: false, releasedRepliesError: error });
      });
  };

  render() {
    this.termsAgreed();
    const todos = this.props.data.currentUser.todos;
    const renderedTodos = this.renderTodoList(todos);

    const empty = (
      <div>
        <Empty title="Waiting for replies!" icon={<Check />} />
      </div>
    );

    return (
      <div>
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            marginTop: 50
          }}
        >
          {this.canRequestContacts(todos) ?
            <TexterRequest
             user={this.props.data.currentUser}
             organizationId={this.props.match.params.organizationId}
            />
            : <Paper>
              <div style={{ padding: "20px" }}>
                <h3>You must complete your current assignments before requesting more contacts</h3>
                <p>
                  You can request more contacts when you have fewer than ${ASSIGNMENT_REQUEST_BLOCK_UNMESSAGED}
                  unsent initials and fewer than ${ASSIGNMENT_REQUEST_BLOCK_UNREPLIED} unsent replies.
                </p>
              </div>
            </Paper>
          }
        </div>
        {renderedTodos.length === 0 ? empty : renderedTodos}

        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            marginTop: 50
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              flexDirection: "column",
              maxWidth: 300,
              textAlign: "center"
            }}
          >
            {this.state.releasedReplies ? (
              <h1> All gone! </h1>
            ) : this.state.releasedRepliesError ? (
              <p> Error releasing replies: {error} </p>
            ) : (
              [
                <h1 key={1}> Done for the day? </h1>,
                <p key={2}>
                  We can reassign conversations you haven't answered to other
                  available texters
                </p>
              ]
            )}
            <RaisedButton
              primary={true}
              fullWidth
              disabled={
                this.state.releasingReplies || this.state.releasedReplies
              }
              onClick={this.releaseMyReplies}
            >
              Reassign My Replies
            </RaisedButton>
          </div>
        </div>
      </div>
    );
  }
}

TexterTodoList.propTypes = {
  organizationId: PropTypes.string,
  match: PropTypes.object.isRequired,
  history: PropTypes.object.isRequired,
  data: PropTypes.object
};

const queries = {
  data: {
    query: gql`
      query getTodos(
        $organizationId: String!
        $needsMessageFilter: ContactsFilter
        $needsResponseFilter: ContactsFilter
        $badTimezoneFilter: ContactsFilter
        $completedConvosFilter: ContactsFilter
        $pastMessagesFilter: ContactsFilter
        $skippedMessagesFilter: ContactsFilter
      ) {
        currentUser {
          id
          email
          terms
          todos(organizationId: $organizationId) {
            id
            campaign {
              id
              title
              description
              useDynamicAssignment
              hasUnassignedContacts
              introHtml
              primaryColor
              logoImageUrl
              dueBy
            }
            maxContacts
            unmessagedCount: contactsCount(contactsFilter: $needsMessageFilter)
            unrepliedCount: contactsCount(contactsFilter: $needsResponseFilter)
            badTimezoneCount: contactsCount(contactsFilter: $badTimezoneFilter)
            totalMessagedCount: contactsCount(
              contactsFilter: $completedConvosFilter
            )
            pastMessagesCount: contactsCount(
              contactsFilter: $pastMessagesFilter
            )
            skippedMessagesCount: contactsCount(
              contactsFilter: $skippedMessagesFilter
            )
          }
        }
      }
    `,
    options: ownProps => ({
      variables: {
        organizationId: ownProps.match.params.organizationId,
        needsMessageFilter: {
          messageStatus: "needsMessage",
          isOptedOut: false,
          validTimezone: true
        },
        needsResponseFilter: {
          messageStatus: "needsResponse",
          isOptedOut: false,
          validTimezone: true
        },
        badTimezoneFilter: {
          isOptedOut: false,
          validTimezone: false,
          messageStatus: "needsMessageOrNeedsResponse"
        },
        completedConvosFilter: {
          isOptedOut: false,
          validTimezone: true,
          messageStatus: "messaged"
        },
        pastMessagesFilter: {
          messageStatus: "convo",
          isOptedOut: false,
          validTimezone: true
        },
        skippedMessagesFilter: {
          messageStatus: "closed",
          isOptedOut: false,
          validTimezone: true
        }
      },
      fetchPolicy: "network-only",
      pollInterval: 10000
    })
  }
};

const mutations = {
  releaseMyReplies: ownProps => organizationId => ({
    mutation: gql`
      mutation releaseMyReplies($organizationId: String!) {
        releaseMyReplies(organizationId: $organizationId)
      }
    `,
    variables: { organizationId },
    refetchQueries: ["getTodos"]
  })
};

export default compose(
  withRouter,
  loadData({
    queries,
    mutations
  })
)(TexterTodoList);
