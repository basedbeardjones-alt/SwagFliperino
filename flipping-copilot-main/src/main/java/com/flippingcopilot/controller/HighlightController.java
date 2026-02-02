package com.flippingcopilot.controller;

import com.flippingcopilot.config.FlippingCopilotConfig;
import com.flippingcopilot.model.*;
import com.flippingcopilot.ui.WidgetHighlightOverlay;
import lombok.RequiredArgsConstructor;
import net.runelite.api.Client;
import net.runelite.api.ItemComposition;
import net.runelite.api.VarClientStr;
import net.runelite.api.widgets.ComponentID;
import net.runelite.api.widgets.Widget;
import net.runelite.client.callback.ClientThread;
import net.runelite.client.ui.overlay.OverlayManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.function.Supplier;

import static net.runelite.api.VarPlayer.CURRENT_GE_ITEM;
import static net.runelite.api.Varbits.GE_OFFER_CREATION_TYPE;

@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class HighlightController {

    // dependencies
    private final FlippingCopilotConfig config;
    private final SuggestionManager suggestionManager;
    private final GrandExchange grandExchange;
    private final AccountStatusManager accountStatusManager;
    private final Client client;
    private final OfferManager offerManager;
    private final OverlayManager overlayManager;
    private final HighlightColorController highlightColorController;
    private final ClientThread clientThread;

    // state
    private final ArrayList<WidgetHighlightOverlay> highlightOverlays = new ArrayList<>();

    public void redraw() {
        removeAll();
        if(!config.suggestionHighlights()) {
            return;
        }
        if(!grandExchange.isOpen()) {
            return;
        }
        if (offerManager.isOfferJustPlaced()) {
            return;
        }
        if(suggestionManager.getSuggestionError() != null) {
            return;
        }
        Suggestion suggestion = suggestionManager.getSuggestion();
        if (suggestion == null) {
            return;
        }
        if (grandExchange.isHomeScreenOpen()) {
            drawHomeScreenHighLights(suggestion);
        } else if (grandExchange.isSlotOpen()) {
            drawOfferScreenHighlights(suggestion);
        }
    }

    private void drawHomeScreenHighLights(Suggestion suggestion) {
        boolean isDumpSuggestion = suggestion.isDumpSuggestion();
        Supplier<Color> blueHighlight = () -> highlightColorController.getBlueColor(isDumpSuggestion);
        Supplier<Color> redHighlight = () -> highlightColorController.getRedColor(isDumpSuggestion);

        // ✅ solid inventory highlight (no pulsing)
        Supplier<Color> invHighlight = () -> highlightColorController.getInventorySolidColor();

        AccountStatus accountStatus = accountStatusManager.getAccountStatus();
        if (accountStatus.isCollectNeeded(suggestion)) {
            Widget collectButton = grandExchange.getCollectButton();
            if (collectButton != null) {
                add(collectButton, blueHighlight, new Rectangle(2, 1, 81, 18));
            }
        }
        else if (suggestion.getType().equals("abort")) {
            Widget slotWidget = grandExchange.getSlotWidget(suggestion.getBoxId());
            add(slotWidget, redHighlight);
        }
        else if (suggestion.getType().equals("buy")) {
            int slotId = accountStatus.findEmptySlot();
            if (slotId != -1) {
                Widget buyButton = grandExchange.getBuyButton(slotId);
                if (buyButton != null && !buyButton.isHidden()) {
                    add(buyButton, blueHighlight, new Rectangle(0, 0, 45, 44));
                }
            }
        }
        else if (suggestion.getType().equals("sell")) {
            Widget itemWidget = getInventoryItemWidget(suggestion.getItemId());
            if (itemWidget != null && !itemWidget.isHidden()) {
                // ✅ CHANGED: inventory highlight uses solid color now
                add(itemWidget, invHighlight, new Rectangle(0, 0, 34, 32));
            }
        }
    }

    private void drawOfferScreenHighlights(Suggestion suggestion) {
        boolean isDumpSuggestion = suggestion.isDumpSuggestion();
        Supplier<Color> blueHighlight = () -> highlightColorController.getBlueColor(isDumpSuggestion);
        Widget offerTypeWidget = grandExchange.getOfferTypeWidget();
        String offerType = client.getVarbitValue(GE_OFFER_CREATION_TYPE) == 1 ? "sell" : "buy";
        if (offerTypeWidget != null) {
            boolean offerTypeMatches = offerType.equals(suggestion.getType());
            int currentItemId = client.getVarpValue(CURRENT_GE_ITEM);
            boolean itemMatches = currentItemId == suggestion.getItemId();
            boolean searchOpen = isSearchOpen();
            if (offerTypeMatches) {
                if (itemMatches) {
                    if (offerDetailsCorrect(suggestion)) {
                        highlightConfirm(blueHighlight);
                    } else {
                        if (grandExchange.getOfferPrice() != suggestion.getPrice()) {
                            highlightPrice(blueHighlight);
                        }
                        highlightQuantity(suggestion, blueHighlight);
                    }
                } else if (currentItemId == -1 && searchOpen) {
                    highlightItemInSearch(suggestion, blueHighlight);
                }
            }
            // Check if unsuggested item/offer type is selected
            if (currentItemId != -1
                    && (!offerTypeMatches || (!searchOpen && !itemMatches))
                    && currentItemId == offerManager.getViewedSlotItemId()
                    && offerManager.getViewedSlotItemPrice() > 0) {
                if (grandExchange.getOfferPrice() == offerManager.getViewedSlotItemPrice()) {
                    highlightConfirm(blueHighlight);
                } else {
                    highlightPrice(blueHighlight);
                }
            }

            if (shouldHighlightBack(suggestion, offerTypeMatches, itemMatches, currentItemId, searchOpen)) {
                highlightBackButton(blueHighlight);
            }
        }
    }

    private boolean shouldHighlightBack(Suggestion suggestion, boolean offerTypeMatches, boolean itemMatches, int currentItemId, boolean searchOpen) {
         if (suggestion.getType().equals("wait")) {
             return false;
         }
         if (!offerTypeMatches) {
             return true;
         }
         if (!itemMatches && currentItemId != -1 && !searchOpen) {
             return true;
         }
         if (suggestion.getType().equals("sell") && currentItemId == -1) {
             return true;
         }
         if (suggestion.getType().equals("buy")) {
             AccountStatus accountStatus = accountStatusManager.getAccountStatus();
             if (accountStatus.isCollectNeeded(suggestion)) {
                 return true;
             }
         }
         return false;
    }

    private void highlightItemInSearch(Suggestion suggestion, Supplier<Color> colorSupplier) {
        if (!client.getVarcStrValue(VarClientStr.INPUT_TEXT).isEmpty()) {
            return;
        }
        Widget searchResults = client.getWidget(ComponentID.CHATBOX_GE_SEARCH_RESULTS);
        if (searchResults == null) {
            return;
        }
        Widget[] dynamicChildren = searchResults.getDynamicChildren();
        if (dynamicChildren == null) {
            return;
        }
        for (Widget w : dynamicChildren) {
            if (w == null || w.isHidden()) {
                continue;
            }
            if (w.getItemId() == suggestion.getItemId()) {
                add(w, colorSupplier, new Rectangle(0, 0, w.getWidth(), w.getHeight()));
                return;
            }
        }
    }

    private void highlightConfirm(Supplier<Color> colorSupplier) {
        Widget confirmButton = grandExchange.getConfirmButton();
        if (confirmButton != null) {
            add(confirmButton, colorSupplier);
        }
    }

    private void highlightBackButton(Supplier<Color> colorSupplier) {
        Widget backButton = grandExchange.getBackButton();
        if (backButton != null) {
            add(backButton, colorSupplier);
        }
    }

    private void highlightPrice(Supplier<Color> colorSupplier) {
        Widget priceBtn = grandExchange.getSetPriceButton();
        if (priceBtn != null) {
            add(priceBtn, colorSupplier);
        }
    }

    private void highlightQuantity(Suggestion suggestion, Supplier<Color> colorSupplier) {
        Widget qtyBtn = suggestion.getQuantity() == 0 ? grandExchange.getSetQuantityAllButton() : grandExchange.getSetQuantityButton();
        if (qtyBtn != null) {
            add(qtyBtn, colorSupplier);
        }
    }

    private boolean offerDetailsCorrect(Suggestion suggestion) {
        if (suggestion.getQuantity() != 0 && suggestion.getQuantity() != grandExchange.getOfferQuantity()) {
            return false;
        }
        return suggestion.getPrice() == grandExchange.getOfferPrice();
    }

    private boolean isSearchOpen() {
        Widget w = client.getWidget(ComponentID.CHATBOX_GE_SEARCH_RESULTS);
        return w != null && !w.isHidden();
    }

    public void add(Widget widget, Supplier<Color> colorSupplier) {
        add(widget, colorSupplier, new Rectangle(0, 0, widget.getWidth(), widget.getHeight()));
    }

    public void add(Widget widget, Supplier<Color> colorSupplier, Rectangle rectangle) {
        if (widget == null || widget.isHidden()) {
            return;
        }
        WidgetHighlightOverlay overlay = new WidgetHighlightOverlay(widget, colorSupplier, rectangle);
        highlightOverlays.add(overlay);
        overlayManager.add(overlay);
    }

    public void removeAll() {
        SwingUtilities.invokeLater(() -> {
            highlightOverlays.forEach(overlayManager::remove);
            highlightOverlays.clear();
        });
    }

    private Widget getInventoryItemWidget(int unnotedItemId) {
        // Inventory has a different widget if GE is open
        Widget inventory = client.getWidget(467, 0);
        if (inventory == null) {
            inventory = client.getWidget(149, 0);
            if (inventory == null) {
                return null;
            }
        }

        Widget notedWidget = null;
        Widget unnotedWidget = null;

        for (Widget widget : inventory.getDynamicChildren()) {
            int itemId = widget.getItemId();
            ItemComposition itemComposition = client.getItemDefinition(itemId);

            if (itemComposition.getNote() != -1) {
                if (itemComposition.getLinkedNoteId() == unnotedItemId) {
                    notedWidget = widget;
                }
            } else {
                if (itemId == unnotedItemId) {
                    unnotedWidget = widget;
                }
            }
        }

        return unnotedWidget != null ? unnotedWidget : notedWidget;
    }
}
