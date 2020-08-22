import React, { Component } from "react";
import PropTypes from "prop-types";

import Card from "material-ui/Card"
import CardActions from "material-ui/Card/CardActions"
import CardText from "material-ui/Card/CardText"
import Paper from "material-ui/Paper";
import Chip from "material-ui/Chip";
import RaisedButton from "material-ui/RaisedButton";
import CheckCircleIcon from "material-ui/svg-icons/action/check-circle";
import BlockIcon from "material-ui/svg-icons/content/block";
import CreateIcon from "material-ui/svg-icons/content/create";
import DeleteForeverIcon from "material-ui/svg-icons/action/delete-forever";
import { red500 } from "material-ui/styles/colors";

const styles = {
  wrapper: {
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "flex-start"
  },
  card: {
    margin: 10,
    padding: 10
  },
  chip: {
    marginRight: "auto",
    color: "#000000"
  },
  description: {
    maxWidth: "200px",
    wordWrap: "break-word"
  }
};

class TagEditorList extends Component {
  createHandleEditTag = tagId => () => this.props.oEditTag(tagId);
  createHandleDeleteTag = tagId => () => this.props.onDeleteTag(tagId);

  truncateText = (text, maxLen) => {
    if (text.length < maxLen) {
      return text
    }
    return text.substring(0, maxLen) + "..."
  }

  render() {
    const { tags } = this.props;

    return (
      <div style={styles.wrapper}>
        {tags.map(tag => (
          <Card key={tag.id} style={styles.card}>
            <CardText>
              <div style={{ display: "flex" }}>
                <Chip backgroundColor={"#DDEEEE"} style={styles.chip}>
                  {tag.title}
                </Chip>
              </div>
              {tag.description && (
                <p style={styles.description}>{tag.description}</p>
              )}
              <p>
                Assignable?{" "}
                {tag.isAssignable ? <CheckCircleIcon /> : <BlockIcon />}
              </p>
              <p style={styles.description}>
                {tag.onApplyScript && this.truncateText(tag.onApplyScript, 100)}
              </p>
              {tag.isSystem && <p>System tags cannot be edited</p>}

            </CardText>
            <CardActions>
              <RaisedButton
                label="Edit"
                labelPosition="before"
                disabled={tag.isSystem}
                primary={true}
                icon={<CreateIcon />}
                style={{ marginRight: 10 }}
                onClick={this.createHandleEditTag(tag.id)}
              />
              <RaisedButton
                label="Delete"
                labelPosition="before"
                disabled={tag.isSystem}
                icon={
                  <DeleteForeverIcon
                    color={!tag.isSystem ? red500 : undefined}
                  />
                }
                onClick={this.createHandleDeleteTag(tag.id)}
              />
            </CardActions>

          </Card>
        ))}
      </div>
    );
  }
}

TagEditorList.defaultProps = {};

TagEditorList.propTypes = {
  tags: PropTypes.arrayOf(PropTypes.object).isRequired,
  oEditTag: PropTypes.func.isRequired,
  onDeleteTag: PropTypes.func.isRequired
};

export default TagEditorList;
