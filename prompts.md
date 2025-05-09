# Item Verification App Prompts

This document contains all the essential prompts used in the Item Verification App. These prompts are crucial for rebuilding the application and maintaining consistency in AI interactions.

## Core Prompts

### Item Verification Prompt
This prompt is used when verifying items against the database:
```
Given the following item details:
[Item Details]

Please verify:
1. If the item exists in our database
2. If the details match our records
3. Any discrepancies found
4. Suggested actions

Please provide a structured response with your findings.
```

### Image Analysis Prompt
This prompt is used for analyzing uploaded images:
```
Please analyze this image and:
1. Identify the main item
2. Extract any visible text or labels
3. Note the condition of the item
4. Flag any potential issues or concerns
5. Compare with provided specifications

Provide a detailed analysis focusing on verification aspects.
```

### Data Validation Prompt
Used for validating input data before processing:
```
Please validate the following data:
[Input Data]

Check for:
1. Required fields presence
2. Data format correctness
3. Value ranges
4. Consistency with business rules
5. Potential data quality issues

Provide validation results and any necessary corrections.
```

### Work Prompt
This prompt is used to guide the AI assistant in implementing new features or modifications:
```
<user_query>
[User's request or question]
</user_query>

The AI assistant should:
1. Understand the request and its context
2. Use available tools to gather necessary information
3. Make appropriate code changes or provide relevant information
4. Follow best practices for code generation and modification
5. Ensure changes are compatible with existing codebase
6. Document any significant changes or decisions
```

## Usage Guidelines

1. Always maintain the exact structure and formatting of these prompts
2. Include all specified check points in the responses
3. Keep responses clear, structured, and actionable
4. Flag any uncertainties or ambiguities for human review

## Maintenance Notes

- Update this document when new prompts are added or existing ones are modified
- Include version history for major prompt changes
- Document any specific use cases or limitations for each prompt
- Keep examples updated with current business rules and requirements

## Version History

### Version 1.0 (March 26, 2024)
- Initial documentation of core prompts
- Added Item Verification, Image Analysis, and Data Validation prompts

### Version 1.1 (March 26, 2024)
- Added Work Prompt for guiding AI assistant in development tasks
- Added Version History section
- Updated Last Updated date

---
Last Updated: March 26, 2024
Version: 1.1 