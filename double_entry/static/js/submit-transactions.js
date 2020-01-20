function collectTransactions(elementId) {
    let collection = $(`#${elementId}`);
    let pipelineSectionId = collection.attr('data-pipeline-section-id');
    return $(`#${elementId} .resolved-transaction`).map(function() {
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
        return row_data;
    }).get();
}

function processResponse({transaction_id, errors, warnings, verdict}) {
    // update with feedback from api
    let successFeedback = "";
    let verdict_class;
    switch(verdict) {
        case 0:
            successFeedback = '<span class="fas fa-check text-success"></span>';
            verdict_class = 'verdict-commit';
            break;
        case 1:
            verdict_class = 'verdict-suggest-skip';
            break;
        case 3:
            verdict_class = 'verdict-discard';
            break;
    }

    // TODO: make this look nice
    let errorFeedback = "";
    if(errors)
        errorFeedback = `<ul class="transaction-errors">${errors.map(err => `<li>${err}</li>`).concat()}</ul>`;
    let warningFeedback = "";
    if(warnings)
        warningFeedback = `<ul class="transaction-warnings">${warnings.map(err => `<li>${err}</li>`).concat()}</ul>`;
    let element = $(`#${transaction_id} > .transaction-feedback`);
    element.addClass(verdict_class);
    let feedback_html = [successFeedback,warningFeedback,errorFeedback].join('<br/>');
    element.html(feedback_html);
}

function submitTransactions(endpointUrl, elementIds, commit=true, responseCallback=processResponse) {
    let transactionLists = elementIds.map(collectTransactions);
    let transactions = [].concat.apply([], transactionLists);
    let postData = { commit: commit, transactions: transactions };
    $.ajax({
        url: endpointUrl, method: "post", dataType: "json",
        data: JSON.stringify(postData)
    }).done(function ({pipeline_responses}) {
        $.each(pipeline_responses,responseCallback);
    });
}