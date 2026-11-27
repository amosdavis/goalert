import { Chance } from 'chance'
import { testScreen } from '../support/e2e'
const c = new Chance()

function testServices(screen: ScreenFormat): void {
  beforeEach(() => {
    window.localStorage.setItem('show_services_new_feature_popup', 'false')
  })

  describe('Labels', () => {
    beforeEach(() =>
      cy.createLabel().then((l: Label) => {
        return cy.visit(`/services/${l.svcID}/labels`)
      }),
    )

    it('should not be able to create a label when DisableLabelCreation is true', () => {
      const randomWord = c.word({
        length: 7,
      })
      if (screen === 'mobile') {
        cy.pageFab()
      } else {
        cy.get('button[data-testid="create-label"]').click()
      }
      cy.get('input[name=key]').findByLabel(`Create "${randomWord}"`)

      cy.updateConfig({
        General: {
          DisableLabelCreation: true,
        },
      })
      cy.reload()

      if (screen === 'mobile') {
        cy.pageFab()
      } else {
        cy.get('button[data-testid="create-label"]').click()
      }
      cy.get('input[name=key]').type(`Create "${randomWord}"`)
      cy.get('[data-cy="select-dropdown"]').should('contain', 'No options')
    })
  })
}

testScreen('Services', testServices)
