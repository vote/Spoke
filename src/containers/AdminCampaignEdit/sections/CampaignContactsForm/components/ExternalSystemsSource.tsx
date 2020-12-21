import React from "react";
import gql from "graphql-tag";
import moment from "moment";

import Avatar from "material-ui/Avatar";
import DropDownMenu from "material-ui/DropDownMenu";
import MenuItem from "material-ui/MenuItem";
import { Card, CardHeader, CardText } from "material-ui/Card";
import { green500, grey500 } from "material-ui/styles/colors";

import { loadData } from "../../../../hoc/with-operations";
import { ExternalSystem } from "../../../../../api/external-system";

interface Props {
  systemId: string;
  selectedListId?: string;
  onChangeExternalList(listId?: string): void;

  // HOC props
  externalLists: {
    externalSystem: ExternalSystem;
  };
}

export const ExternalSystemsSource: React.SFC<Props> = props => {
  const handleSelectList = (
    _event: React.SyntheticEvent<{}>,
    _index: number,
    listId: string
  ) => {
    const { selectedListId } = props;
    props.onChangeExternalList(selectedListId === listId ? undefined : listId);
  };

  const {
    selectedListId,
    externalLists: { externalSystem }
  } = props;

  return (
    <Card expandable={false} expanded={false}>
      <CardHeader
        title={externalSystem.name}
        subtitle={`Lists last pulled: ${
          externalSystem.syncedAt
            ? moment(externalSystem.syncedAt).fromNow()
            : "never"
        }`}
        avatar={
          <Avatar
            backgroundColor={
              externalSystem.lists.edges.length > 0 ? green500 : grey500
            }
          >
            {externalSystem.lists.edges.length > 9
              ? "9+"
              : externalSystem.lists.edges.length}
          </Avatar>
        }
        showExpandableButton={false}
      />

      {externalSystem.lists.pageInfo.totalCount > 0 && (
        <CardText>
          Choose a list:<br />
          <DropDownMenu
            value={selectedListId}
            onChange={handleSelectList}
            style={{ width: "50%" }}
          >
            {externalSystem.lists.edges.map(({ node: list }) => (
              <MenuItem
                key={list.externalId}
                value={list.externalId}
                primaryText={`${list.name} (${list.listCount} contacts)`}
              />
            ))}
          </DropDownMenu>
        </CardText>
      )}
    </Card>
  );
};

const queries = {
  externalLists: {
    query: gql`
      query getExternalLists($systemId: String!) {
        externalSystem(systemId: $systemId) {
          id
          name
          type
          apiKey
          createdAt
          updatedAt
          syncedAt
          lists {
            pageInfo {
              totalCount
            }
            edges {
              node {
                externalId
                name
                description
                listCount
                doorCount
                createdAt
                updatedAt
              }
            }
          }
        }
      }
    `,
    options: (ownProps: Props) => ({
      variables: {
        systemId: ownProps.systemId
      }
    })
  }
};

export default loadData({
  queries
})(ExternalSystemsSource);
