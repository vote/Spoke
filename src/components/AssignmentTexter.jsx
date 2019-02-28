import PropTypes from 'prop-types'
import React from 'react'
import { ToolbarTitle } from 'material-ui/Toolbar'
import IconButton from 'material-ui/IconButton/IconButton'
import NavigateBeforeIcon from 'material-ui/svg-icons/image/navigate-before'
import NavigateNextIcon from 'material-ui/svg-icons/image/navigate-next'
import AssignmentTexterContact from '../containers/AssignmentTexterContact'
import { StyleSheet, css } from 'aphrodite'
import { withRouter } from 'react-router'
import Check from 'material-ui/svg-icons/action/check-circle'
import Empty from '../components/Empty'
import RaisedButton from 'material-ui/RaisedButton'
import gql from 'graphql-tag'
import loadData from '../containers/hoc/load-data'
import wrapMutations from '../containers/hoc/wrap-mutations'
import { log } from '../lib/log'
const SEND_DELAY = 100

const styles = StyleSheet.create({
  container: {
    position: 'fixed',
    top: 0,
    left: 0,
    // right: 0,
    // bottom: 0
    width: '100%',
    height: '100%',
    zIndex: 1002,
    backgroundColor: 'white',
    overflow: 'hidden'
  },
  navigationToolbarTitle: {
    fontSize: '12px'
  }
})

class AssignmentTexter extends React.Component {
  constructor(props) {
    super(props)

    this.state = {
      // currentContactIndex: 0,
      contactCache: {},
      loading: false,
      errors: []
    }
  }

  componentWillMount() {
    this.updateCurrentContactIndex(0)
  }

  componentWillUpdate(nextProps, nextState) {
    if (this.contactCount() === 0) {
      setTimeout(() => window.location.reload(), 5000)
    }

    // When we send a message that changes the contact status,
    // then if parent.refreshData is called, then props.contactIds
    // will return a new list with the last contact removed and
    // presumably our currentContactIndex will be off.
    // In fact, without the code below, we will 'double-jump' each message
    // we send or change the status in some way.
    // Below, we update our index with the contact that matches our current index.
    if (typeof nextState.currentContactIndex !== 'undefined'
        && nextState.currentContactIndex === this.state.currentContactIndex
        && nextProps.contactIds.length !== this.props.contactIds.length
        && this.props.contactIds[this.state.currentContactIndex]) {
      const curId = this.props.contactIds[this.state.currentContactIndex]
      const nextIndex = nextProps.contactIds.indexOf(curId)
      if (nextIndex !== nextState.currentContactIndex) {
        // eslint-disable-next-line no-param-reassign
        nextState.currentContactIndex = nextIndex
      }
    }
  }
  /*
    getContactData is a place where we've hit scaling issues in the past, and will be important
    to think carefully about for scaling considerations in the future.

    As p2ptextbanking work scales up, texters will contact more people, and so the number of
    contacts in a campaign and the frequency at which we need to get contact data will increase.

    Previously, when the texter clicked 'next' from the texting screen, we'd load a list of contact metadata
    to text next, and then as this data was rendered into the AssignmentTexter and AssignmentTexterContact
    containers, we'd load up each contact in the list separately, doing an API call and database query
    for each contact. For each of the O(n) contacts to text, in aggregate this yielded O(n^2) API calls and
    database queries.

    This round of changes is a mostly client-side structural optimization that will make it so that
    O(n) contacts to text results in O(n) queries. There will also be later rounds of server-side optimization.

    You'll also see references to "contact cache" below--
    this is different than a redis cache, and it's a reference to this component storing contact data in its state,
    which is a form of in-memory client side caching. A blended set of strategies -- server-side optimization,
    getting data from the data store in batches, and storing batches in the component that
    is rendering this data-- working in concert will be key to achieving our scaling goals.

    In addition to getting all the contact data needed to text contacts at once instead of in a nested loop,
    these changes get a batch of contacts at a time with a moving batch window. BATCH_GET is teh number of contacts
    to get at a time, and BATCH_FORWARD is how much before the end of the batch window to prefetch the next batch.

    Example with BATCH_GET = 10 and BATCH_FORWARD = 5 :
      - starting out in the contact list, we get contacts 0-9, and then get their associated data in batch
      via this.props.loadContacts(getIds)
      - texter starts texting through this list, texting contact 0, 1, 2, 3. we do not need to make any more
      API or database calls because we're using data we already got and stored in this.state.contactCache
      - when the texter gets to contact 4, contacts[newIndex + BATCH_FORWARD] is now false, and thts tells us we
      should get the next batch, so the next 10 contacts (10-19) are loaded up into this.state.contactCache
      - when the texter gets to contact 10, contact 10 has already been loaded up into this.state.contactCache and
      so the texter wont likely experience a data loading delay

    getContactData runs when the user clicks the next arrow button on the contact screen.

  */
  getContactData = async (newIndex, force = false) => {
    const { contactIds } = this.props
    const BATCH_GET = 10 // how many to get at once
    const BATCH_FORWARD = 5 // when to reach out and get more
    let getIds = []
    // if we don't have current data, get that
    if (contactIds[newIndex]
        && !this.state.contactCache[contactIds[newIndex]]) {
      getIds = contactIds
        .slice(newIndex, newIndex + BATCH_GET)
        .filter((cId) => !force || !this.state.contactCache[cId])
    }
    // if we DO have current data, but don't have data base BATCH_FORWARD...
    if (!getIds.length
        && contactIds[newIndex + BATCH_FORWARD]
        && !this.state.contactCache[contactIds[newIndex + BATCH_FORWARD]]) {
      getIds = contactIds
        .slice(newIndex + BATCH_FORWARD, newIndex + BATCH_FORWARD + BATCH_GET)
        .filter((cId) => !force || !this.state.contactCache[cId])
    }

    if (getIds.length) {
      this.setState({ loading: true })
      const contactData = await this.props.loadContacts(getIds)
      const { data: { getAssignmentContacts } } = contactData
      if (getAssignmentContacts) {
        const newContactData = {}
        getAssignmentContacts.forEach(newContact => {
          newContactData[newContact.id] = newContact
        })
        this.setState({
          loading: false,
          contactCache: { ...this.state.contactCache,
                          ...newContactData } })
      }
    }
  }

  getContactId = (contactIds, index) => {
    if (contactIds.length > index) {
      return contactIds[index]
    }
    return null
  }

  incrementCurrentContactIndex = (increment) => {
    let newIndex = this.state.currentContactIndex
    newIndex = newIndex + increment
    this.updateCurrentContactIndex(newIndex)
  }

  updateCurrentContactIndex = (newIndex) => {
    this.setState({
      currentContactIndex: newIndex
    })
    this.getContactData(newIndex)
  }

  hasPrevious = () => {
    return this.state.currentContactIndex > 0
  }

  hasNext = () => {
    return this.state.currentContactIndex < this.contactCount() - 1
  }

  handleFinishContact = () => {
    if (this.hasNext()) {
      this.handleNavigateNext()
    } else {
      // Will look async and then redirect to todo page if not
      this.props.assignContactsIfNeeded(/* checkServer*/true)
    }
  }

  handleNavigateNext = () => {
    if (!this.hasNext()) {
      return
    }

    this.incrementCurrentContactIndex(1)
  }

  handleNavigatePrevious = () => {
    if (!this.hasPrevious()) {
      return
    }
    this.incrementCurrentContactIndex(-1)
  }

  handleCannedResponseChange = (script) => {
    this.handleScriptChange(script)
    this.handleClosePopover()
  }

  handleScriptChange = (script) => {
    this.setState({ script })
  }

  handleExitTexter = () => {
    this.props.router.push('/app/' + (this.props.organizationId || ''))
  }

  contactCount = () => {
    const { contactIds } = this.props
    return contactIds.length
  }

  currentContact = () => {
    const { contactIds } = this.props

    // If the index has got out of sync with the contacts available, then rewind to the start
    if (typeof this.state.currentContactIndex !== 'undefined') {
      return this.getContactId(contactIds, this.state.currentContactIndex)
    }

    this.updateCurrentContactIndex(0)
    return this.getContactId(contactIds, 0)
  }

  renderNavigationToolbarChildren = () => {
    const { allContactsCount } = this.props
    const remainingContacts = this.contactCount()
    const messagedContacts = allContactsCount - remainingContacts

    const currentIndex = this.state.currentContactIndex + 1 + messagedContacts
    let ofHowMany = allContactsCount
    if (ofHowMany === currentIndex
        && this.props.assignment.campaign.useDynamicAssignment) {
      ofHowMany = '?'
    }
    const title = `${currentIndex} of ${ofHowMany}`
    return [
      <ToolbarTitle
        key='title'
        className={css(styles.navigationToolbarTitle)}
        text={title}
      />,
      <IconButton
        key='previous'
        onTouchTap={this.handleNavigatePrevious}
        disabled={!this.hasPrevious()}
      >
        <NavigateBeforeIcon />
      </IconButton>,
      <IconButton
        key='next'
        onTouchTap={this.handleNavigateNext}
        disabled={!this.hasNext()}
      >
        <NavigateNextIcon />
      </IconButton>
    ]
  }

  sendMessage = (contact_id, payload) => {
    const isLastOne = !this.hasNext()

    const promises = []

    if (payload.message) 
      promises.push(
        this.props.mutations.sendMessage(payload.message, contact_id)
          .then(response => {
            if (response.errors) throw new Error(response.errors)
            log.info(`Successfully send message to ${contact_id}`) 
          })
          .catch(this.handleSendMessageError(contact_id))
      )

    console.log(payload)

    if (payload.questionResponseObjects)
      promises.push(this.props.mutations.updateQuestionResponses(payload.questionResponseObjects, contact_id)
          .then(response => { 
            log.info(`Successfully recorded question response: ${JSON.stringify(response)}`) 
          })
      )

    if (payload.deletionIds)
      promises.push(this.props.mutations.deleteQuestionResponses(payload.deletionIds, contact_id).then(
        response => {
            log.info(`Successfully deleted question response: ${JSON.stringify(response)}`) 
        }
      ))

    if (payload.optOut)
      promises.push(this.props.mutations.createOptOut(payload.optOut, contact_id).then(response => {
            log.info(`Successfully recorded question response: ${JSON.stringify(response)}`) 
      }))

    console.log(promises)
    
    Promise.all(promises)
      .then(results => { 
        console.log(results)
        log.info(`Successfully recorded all info for ${contact_id}`)
        if (isLastOne) this.handleFinishContact()
      })

    if (!isLastOne) {
      setTimeout(() => this.handleFinishContact(), SEND_DELAY)
    }
  }

  goBackToTodos = () => {
    const { campaign } = this.props.assignment
    this.props.router.push(`/app/${campaign.organization.id}/todos`)
  }

  handleSendMessageError = (contact_id) => (e) => {
    let error = {id: contact_id}

    if (e.status === 402) {
      this.goBackToTodos()
    } else {
      error.snackbarError = e.message

      if (e.message.includes('Your assignment has changed')) {
        error.snackbarActionTitle = 'Back to todos'
        error.snackbarOnTouchTap = this.goBackToTodos
      } else if (e.message.includes('Skipped sending because this contact was already opted out')) {
        // opt out or send message Error
        error.snackbarActionTitle = 'A previous contact had been opted out'
      } else {
        error.snackbarError = 'Error: Please wait a few seconds and try again.'
      }

      error.snackbarError = `Error for contact ${contact_id}: ${error.snackbarError.replace('Error: GraphQL error:', '')}`

      this.setState({ errors: this.state.errors.concat([error]) })

      setTimeout(() => {
        this.setState({ errors: this.state.errors.filter(e => e.id !== contact_id) })
      }, 2000)
      
      throw e;
    }
  }

  renderTexter = () => {
    const { errors } = this.state
    const { assignment } = this.props
    const { campaign, texter } = assignment
    const contactId = this.currentContact()
    const navigationToolbarChildren = this.renderNavigationToolbarChildren()
    const contactData = this.state.contactCache[contactId]
    if (!contactData) {
      const self = this
      setTimeout(() => {
        if (self.state.contactCache[contactId]) {
          self.forceUpdate()
        } else if (!self.state.loading) {
          self.updateCurrentContactIndex(self.state.currentContactIndex)
        }
      }, 200)
      return null
    }
    return (
      <AssignmentTexterContact
        key={contactId}
        assignment={assignment}
        campaignContactId={contactId}
        contact={contactData}
        texter={texter}
        campaign={campaign}
        navigationToolbarChildren={navigationToolbarChildren}
        onFinishContact={this.handleFinishContact}
        refreshData={this.props.refreshData}
        onExitTexter={this.handleExitTexter}
        errors={errors}
        mutations={{
          editCampaignContactMessageStatus: this.props.mutations.editCampaignContactMessageStatus,
          bulkSendMessages: this.props.mutations.bulkSendMessages,
        }}
        sendMessage={this.sendMessage}
      />
    )
  }
  renderEmpty = () => {
    return (
      <div>
        <Empty
          title="You've already messaged or replied to all your assigned contacts for now."
          icon={<Check />}
          content={(<RaisedButton
            label='Back To Todos'
            onClick={this.handleExitTexter}
          />)}
        />
      </div>
    )
  }

  render() {
    const { contactIds } = this.props
    return (
      <div className={css(styles.container)}>
        {contactIds.length === 0 ? this.renderEmpty() : this.renderTexter()}
      </div>
    )
  }
}

AssignmentTexter.propTypes = {
  currentUser: PropTypes.object,
  assignment: PropTypes.object,      // current assignment
  contactIds: PropTypes.arrayOf(PropTypes.string),   // contacts for current assignment
  allContactsCount: PropTypes.number,
  router: PropTypes.object,
  refreshData: PropTypes.func,
  loadContacts: PropTypes.func,
  assignContactsIfNeeded: PropTypes.func,
  organizationId: PropTypes.string
}

const mapMutationsToProps = () => ({
  createOptOut: (optOut, campaignContactId) => ({
    mutation: gql`
      mutation createOptOut($optOut: OptOutInput!, $campaignContactId: String!) {
        createOptOut(optOut: $optOut, campaignContactId: $campaignContactId) {
          id
          optOut {
            id
            createdAt
          }
        }
      }
    `,
    variables: {
      optOut,
      campaignContactId
    }
  }),
  editCampaignContactMessageStatus: (messageStatus, campaignContactId) => ({
    mutation: gql`
      mutation editCampaignContactMessageStatus($messageStatus: String!, $campaignContactId: String!) {
        editCampaignContactMessageStatus(messageStatus:$messageStatus, campaignContactId: $campaignContactId) {
          id
          messageStatus
        }
      }
    `,
    variables: {
      messageStatus,
      campaignContactId
    }
  }),
  deleteQuestionResponses: (interactionStepIds, campaignContactId) => ({
    mutation: gql`
      mutation deleteQuestionResponses($interactionStepIds:[String], $campaignContactId: String!) {
        deleteQuestionResponses(interactionStepIds: $interactionStepIds, campaignContactId: $campaignContactId) {
          id
        }
      }
    `,
    variables: {
      interactionStepIds,
      campaignContactId
    }
  }),
  updateQuestionResponses: (questionResponses, campaignContactId) => ({
    mutation: gql`
      mutation updateQuestionResponses($questionResponses:[QuestionResponseInput], $campaignContactId: String!) {
        updateQuestionResponses(questionResponses: $questionResponses, campaignContactId: $campaignContactId) {
          id
        }
      }
    `,
    variables: {
      questionResponses,
      campaignContactId
    }
  }),
  sendMessage: (message, campaignContactId) => ({
    mutation: gql`
      mutation sendMessage($message: MessageInput!, $campaignContactId: String!) {
        sendMessage(message: $message, campaignContactId: $campaignContactId) {
          id
          messageStatus
          messages {
            id
            createdAt
            text
            isFromContact
          }
        }
      }
    `,
    variables: {
      message,
      campaignContactId
    }
  }),
  bulkSendMessages: (assignmentId) => ({
    mutation: gql`
      mutation bulkSendMessages($assignmentId: Int!) {
        bulkSendMessages(assignmentId: $assignmentId) {
          id
        }
      }
    `,
    variables: {
      assignmentId
    }
  })
})

export default loadData(wrapMutations(
  withRouter(AssignmentTexter)), {
    mapMutationsToProps
  })

