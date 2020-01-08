const endpoint_url = $('#pipeline-endpoint-url').attr('data-url');

function collectTransactions(elementId) {
    let collection = $(`#${elementId}`);
    let pipelineSectionId = collection.attr('data-pipeline-section-id');
    return $(`#${elementId} .resolved-transaction`).map(function() {
        let row_id = this.id;
        let row_data =  {
            pipeline_section_id: pipelineSectionId,
            transaction_id: row_id
        };
        $.each(this.attributes, function (attr) {
            if(attr.name.startsWith('data-') && attr.specified) {
                let parameterName = attr.name.substring(5).replace('-', '_');
                row_data[parameterName] = attr.value;
            }
        });
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
        return `<ul class="transaction-errors">${errors.map(err => `<li>${err}</li>`).concat()}</ul>`;
    let warningFeedback = "";
    if(warnings)
        return `<ul class="transaction-warnings">${warnings.map(err => `<li>${err}</li>`).concat()}</ul>`;
    let element = $(`#${transaction_id} > .transaction-feedback`);
    element.addClass(verdict_class);
    element.html(
        [successFeedback,warningFeedback,errorFeedback].join('<br/>')
    );
}

function submitTransactions(elementIds, commit=true, responseCallback=processResponse) {
    let postData = {
        commit: commit,
        transactions: $(elementIds).each(collectTransactions).concat()
    };
    $.ajax({
        url: endpoint_url, method: "post", dataType: "json",
        data: JSON.stringify(postData)
    }).done(function ({pipeline_responses}) {
        $.each(pipeline_responses,responseCallback);
    });
}