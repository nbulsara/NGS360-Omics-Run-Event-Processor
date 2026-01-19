NAME=ngs360-omics-run-event-processor

cf-create:
	aws cloudformation create-stack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json
