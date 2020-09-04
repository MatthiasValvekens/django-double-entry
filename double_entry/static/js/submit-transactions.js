function collectTransactions(elementId) {
    let collection = $(`#${elementId}`);
    let pipelineSectionId = collection.attr('data-pipeline-section-id');
    let to_commit = $(`#${elementId} .resolved-transaction`).filter('[data-commit="true"]');
    return to_commit.map(function() {
        let feedback = this.find('.transaction-feedback')[0];
        if(!feedback.dataset.commit) {
            return null;
        }
        let row_id = this.id;
        let row_data = { transaction_id: row_id };
        // if undefined, we assume that it isn't necessary
        if (typeof pipelineSectionId !== typeof undefined && pipelineSectionId !== false) {
            row_data.pipeline_section_id = pipelineSectionId;
        }
        $.each(this.attributes, function (i, attr) {
            if(attr.name.startsWith('data-') && attr.specified) {
                let parameterName = attr.name.substring(5).replace(/-/g, '_');
                row_data[parameterName] = attr.value;
            }
        });
        if(feedback.dataset.verdict !== 'commit') {
            // This means that the user explicitly asked for this transaction to be committed, even though
            // the server already gave a warning. Hence, we now have to tell the server that non-fatal
            // problems with this entry are to be ignored.
            row_data.do_not_skip = true;
        }
        return row_data;
    }).get();
}

function processResponse({transaction_id, errors, warnings, verdict, committed=false}, commitIntention=false) {
    let element = $(`#${transaction_id}`);
    let elementFeedback = element.find('.transaction-feedback')[0];
    // nonzero verdict and there was intent to commit => server rejected the transaction
    if(committed || (commitIntention && verdict > 0)) {
        // remove item from view, no longer relevant
        element.remove();
        return;
    }
    // update with feedback from api
    switch(verdict) {
        case 0:
            elementFeedback.dataset.verdict = 'commit';
            elementFeedback.dataset.commit = 'true';
            break;
        case 1:
            elementFeedback.dataset.verdict = 'suggest-skip';
            delete elementFeedback.dataset.commit;
            break;
        case 3:
            elementFeedback.dataset.verdict = 'discard';
            delete elementFeedback.dataset.commit;
            break;
    }

    let verdictFeedback = '<span class="verdict-feedback"></span><br/>'
    let errorFeedback = "";
    if(errors.length)
        errorFeedback = `<ul class="transaction-errors">${errors.map(err => `<li>${err}</li>`).concat()}</ul>`;
    let warningFeedback = "";
    if(warnings.length)
        warningFeedback = `<br/><ul class="transaction-warnings">${warnings.map(err => `<li>${err}</li>`).concat()}</ul>`;
    elementFeedback.innerHTML = verdictFeedback + errorFeedback + warningFeedback;
}

function submitTransactions(endpointUrl, elementIds, commit=true, responseCallback=processResponse, extraCallback=null) {
    let transactionLists = elementIds.map(collectTransactions);
    let transactions = [].concat.apply([], transactionLists);
    let postData = { commit: commit, transactions: transactions };
    $.ajax({
        url: endpointUrl, method: "post", dataType: "json",
        data: JSON.stringify(postData)
    }).done(function (response) {
        let {pipeline_responses} = response;
        pipeline_responses.forEach(resp => responseCallback(resp, true));
        if(extraCallback !== null) {
            extraCallback(response);
        }
    });
}