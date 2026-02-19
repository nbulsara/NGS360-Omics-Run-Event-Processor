NAME=ngs360-omics-run-event-processor
BUCKET_PREFIX=omics-run-events

# Include local configuration
-include Makefile.local

test:
	python3 -m pytest -vv --cov ./
	coverage html

create-lambda-package:
	# Get git short hash for versioning
	$(eval VER := $(shell git log -1 --pretty=format:"%h"))
	# Remove zip file if it exists
	$(eval zipfile := $(NAME)-$(VER).zip)
	@if [ -f $(zipfile) ]; then rm $(zipfile); fi

	mkdir -p lambda-package && \
	cd lambda-package && \
	cp ../*.py . && \
	pip3 install -r ../requirements.txt -t . && \
	zip -r ../$(zipfile) .
	aws s3 cp $(zipfile) s3://${BUCKET}/${BUCKET_PREFIX}/$(zipfile) --sse

cf-create: create-lambda-package
	aws cloudformation create-stack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json

cf-update:
	aws cloudformation create-change-set --change-set-name updateStack --stack-name $(NAME) --template-body file://$(NAME).yaml --capabilities CAPABILITY_IAM --parameters file://parameters.json

lambda-update: create-lambda-package
	aws lambda update-function-code --function-name $(STACK_NAME) --s3-bucket ${BUCKET} --s3-key ${BUCKET_PREFIX}/$(zipfile) --publish
